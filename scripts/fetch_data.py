#!/usr/bin/env python3
"""
VarejoRadar-MS — Pipeline ETL
=============================

Gera ``data/estabelecimentos_ms.parquet`` a partir dos **Dados Abertos do CNPJ**
publicados pela Receita Federal do Brasil (RFB), filtrando estabelecimentos
ATIVOS em Mato Grosso do Sul (UF = MS) com CNAE (principal ou secundário) de
supermercados, hipermercados, minimercados e atacadistas de alimentos.

Etapas
------
1. **Download** paralelo (HTTP Range, retomável) de Estabelecimentos*, Empresas*,
   Municipios e Cnaes do compartilhamento público Nextcloud da RFB.
2. **Filtro** em streaming (sem descompactar no disco): UF = MS,
   situação cadastral 02 (ATIVA) e CNAEs-alvo.
3. **Enriquecimento** com razão social e porte (arquivos Empresas*).
4. **Padronização** de município (79 municípios de MS — IBGE/SIAFI), CNPJ,
   CEP, telefones, e-mail e endereço completo.
5. **Geocodificação** via Nominatim/OpenStreetMap (1 req/s, cache incremental
   em ``data/geocache_nominatim.json``) na hierarquia logradouro → bairro →
   centroide do município, sempre com jitter determinístico (hash do CNPJ).
   Coordenadas fora de MS ou distantes da sede municipal são descartadas.

Uso
---
    python scripts/fetch_data.py                  # pipeline completo
    python scripts/fetch_data.py --mes 2026-10    # força o mês de referência
    python scripts/fetch_data.py --skip-download  # reprocessa arquivos já baixados
    python scripts/fetch_data.py --no-geocode     # sem novas consultas (usa só o cache)
    python scripts/fetch_data.py --geo-budget-min 30  # limita o tempo de geocodificação

Fontes
------
- RFB / Dados Abertos CNPJ: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
- Municípios (IBGE + SIAFI + centroides): https://github.com/kelvins/municipios-brasileiros
- Geocodificação: Nominatim / © OpenStreetMap contributors (ODbL)
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import re
import sys
import threading
import time
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --------------------------------------------------------------------------- #
# Configuração
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
OUT_PARQUET = DATA_DIR / "estabelecimentos_ms.parquet"
GEO_CACHE = DATA_DIR / "geocache_nominatim.json"
METADATA = DATA_DIR / "metadata.json"

RFB_HOST = "https://arquivos.receitafederal.gov.br"
RFB_SHARE_TOKEN = "YggdBLfdninEJX9"  # compartilhamento público "dados_abertos_cnpj"
RFB_WEBDAV = f"{RFB_HOST}/public.php/webdav"
MUNICIPIOS_URL = (
    "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_INTERVALO = 1.05  # segundos entre requisições (política de uso OSMF)

UF_ALVO = "MS"
COD_UF_IBGE = 50
SITUACAO_ATIVA = "02"

# CNAE (7 dígitos) -> (segmento, prioridade p/ classificar CNAEs secundários)
CNAES_ALVO: dict[str, tuple[str, int]] = {
    "4711301": ("Hipermercado", 1),
    "4711302": ("Supermercado", 2),
    "4691500": ("Atacadista / Atacarejo", 3),
    "4639701": ("Atacadista / Atacarejo", 4),
    "4639702": ("Atacadista / Atacarejo", 5),
    "4712100": ("Minimercado / Mercearia", 6),
}
DESCRICOES_CNAE_PADRAO = {
    "4711301": "Comércio varejista de mercadorias em geral, com predominância de produtos alimentícios - hipermercados",
    "4711302": "Comércio varejista de mercadorias em geral, com predominância de produtos alimentícios - supermercados",
    "4712100": "Comércio varejista de mercadorias em geral, com predominância de produtos alimentícios - minimercados, mercearias e armazéns",
    "4639701": "Comércio atacadista de produtos alimentícios em geral",
    "4639702": "Comércio atacadista de produtos alimentícios em geral, com atividade de fracionamento e acondicionamento associada",
    "4691500": "Comércio atacadista de mercadorias em geral, com predominância de produtos alimentícios",
}
PORTE_MAP = {"00": "Não informado", "01": "ME", "03": "EPP", "05": "Demais"}

# Layout oficial do arquivo ESTABELECIMENTOS (sem cabeçalho, ';', latin-1)
COLS_ESTAB = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "matriz_filial", "nome_fantasia",
    "situacao_cadastral", "data_situacao", "motivo_situacao", "cidade_exterior",
    "pais", "data_inicio_atividade", "cnae_principal", "cnae_secundaria",
    "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep",
    "uf", "municipio_rfb", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
    "ddd_fax", "fax", "email", "situacao_especial", "data_situacao_especial",
]
COLS_EMPRESAS = [
    "cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel",
    "capital_social", "porte", "ente_federativo",
]

# Caixa envolvente de MS (com folga) para validar coordenadas
MS_BBOX = {"lat_min": -24.2, "lat_max": -17.0, "lon_min": -58.3, "lon_max": -50.8}

CHUNK_BYTES = 16 * 1024 * 1024
DOWNLOAD_WORKERS = int(os.environ.get("VR_DOWNLOAD_WORKERS", "16"))
USER_AGENT = "VarejoRadar-MS-ETL/1.0 (open-data ETL; python-requests)"

SESSION = requests.Session()
SESSION.headers["User-Agent"] = USER_AGENT
_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# 1. Download (WebDAV público da RFB, paralelo por faixas de bytes)
# --------------------------------------------------------------------------- #
def webdav_list(path: str = "") -> list[tuple[str, int | None]]:
    """Lista (nome, tamanho) de um diretório do compartilhamento público."""
    url = f"{RFB_WEBDAV}/{path}".rstrip("/") + "/"
    for tentativa in range(5):
        try:
            r = SESSION.request(
                "PROPFIND", url, auth=(RFB_SHARE_TOKEN, ""),
                headers={"Depth": "1"}, timeout=90,
            )
            r.raise_for_status()
            break
        except requests.RequestException as exc:
            log(f"PROPFIND falhou ({exc}); nova tentativa em {2 ** tentativa}s")
            time.sleep(2 ** tentativa)
    else:
        raise RuntimeError(f"Não foi possível listar {url}")
    itens = []
    for bloco in re.findall(r"<d:response>(.*?)</d:response>", r.text, re.S):
        href = re.search(r"<d:href>([^<]+)</d:href>", bloco).group(1)
        nome = href.rstrip("/").split("/")[-1]
        tam = re.search(r"<d:getcontentlength>(\d+)</d:getcontentlength>", bloco)
        if href.rstrip("/") != url.replace(RFB_HOST, "").rstrip("/"):
            itens.append((nome, int(tam.group(1)) if tam else None))
    return itens


def descobrir_mes_mais_recente() -> str:
    meses = sorted(n for n, _ in webdav_list() if re.fullmatch(r"\d{4}-\d{2}", n))
    if not meses:
        raise RuntimeError("Nenhum mês de referência encontrado no compartilhamento da RFB.")
    return meses[-1]


def _baixar_faixa(url: str, destino: Path, inicio: int, fim: int) -> int:
    for tentativa in range(8):
        try:
            with SESSION.get(
                url, auth=(RFB_SHARE_TOKEN, ""), stream=True, timeout=(30, 120),
                headers={"Range": f"bytes={inicio}-{fim}"},
            ) as r:
                if r.status_code != 206:
                    raise requests.HTTPError(f"status {r.status_code} (esperado 206)")
                buf = r.raw.read(fim - inicio + 1, decode_content=False)
            if len(buf) != fim - inicio + 1:
                raise IOError(f"faixa incompleta: {len(buf)} de {fim - inicio + 1} bytes")
            fd = os.open(destino, os.O_WRONLY)
            try:
                os.pwrite(fd, buf, inicio)
            finally:
                os.close(fd)
            return len(buf)
        except Exception as exc:  # noqa: BLE001 — rede instável: sempre re-tentar
            espera = min(60, 2 ** tentativa)
            log(f"  faixa {destino.name}[{inicio}] erro: {exc}; retry em {espera}s")
            time.sleep(espera)
    raise RuntimeError(f"Falha definitiva baixando {destino.name} bytes {inicio}-{fim}")


def baixar_arquivos(mes: str, nomes: list[str]) -> dict[str, Path]:
    """Baixa arquivos em paralelo (faixas de 16 MB), com retomada via .parts.json."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    tamanhos = dict(webdav_list(mes))
    tarefas, caminhos, estados = [], {}, {}
    total_bytes = 0
    for nome in nomes:
        if nome not in tamanhos:
            raise RuntimeError(f"Arquivo {nome} não encontrado em {mes}.")
        tam = tamanhos[nome] or 0
        destino = RAW_DIR / f"{mes}_{nome}"
        parts_file = destino.with_suffix(".parts.json")
        caminhos[nome] = destino
        if destino.exists() and destino.stat().st_size == tam and not parts_file.exists():
            continue  # já completo
        feitos = set(json.loads(parts_file.read_text())) if parts_file.exists() else set()
        if not destino.exists() or destino.stat().st_size != tam:
            with open(destino, "wb") as f:
                f.truncate(tam)
            feitos = set()
        estados[nome] = {"feitos": feitos, "parts_file": parts_file, "n": 0}
        url = f"{RFB_WEBDAV}/{mes}/{nome}"
        for ini in range(0, tam, CHUNK_BYTES):
            if ini in feitos:
                continue
            fim = min(ini + CHUNK_BYTES, tam) - 1
            tarefas.append((nome, url, destino, ini, fim))
            total_bytes += fim - ini + 1
        estados[nome]["n"] = math.ceil(tam / CHUNK_BYTES) if tam else 0

    if not tarefas:
        log("Todos os arquivos já estão em cache local.")
        return caminhos

    log(f"Baixando {total_bytes / 2**30:.2f} GB em {len(tarefas)} faixas "
        f"({DOWNLOAD_WORKERS} conexões paralelas)…")
    t0, baixado, lock = time.time(), 0, threading.Lock()
    with ThreadPoolExecutor(DOWNLOAD_WORKERS) as pool:
        futs = {pool.submit(_baixar_faixa, url, dst, ini, fim): (nome, ini)
                for nome, url, dst, ini, fim in tarefas}
        for i, fut in enumerate(as_completed(futs), 1):
            nome, ini = futs[fut]
            n = fut.result()
            with lock:
                baixado += n
                est = estados[nome]
                est["feitos"].add(ini)
                est["parts_file"].write_text(json.dumps(sorted(est["feitos"])))
                if len(est["feitos"]) >= est["n"]:
                    est["parts_file"].unlink(missing_ok=True)
                    log(f"  ✔ {nome} concluído")
            if i % 25 == 0 or i == len(tarefas):
                vel = baixado / max(time.time() - t0, 1e-6) / 2**20
                log(f"  {baixado / 2**30:.2f}/{total_bytes / 2**30:.2f} GB  ({vel:.1f} MB/s)")
    return caminhos


# --------------------------------------------------------------------------- #
# 2. Utilitários de padronização
# --------------------------------------------------------------------------- #
def sem_acento(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", str(txt))
    return "".join(c for c in txt if not unicodedata.combining(c)).upper().strip()


def limpar(txt) -> str:
    if txt is None or (isinstance(txt, float) and math.isnan(txt)):
        return ""
    return re.sub(r"\s+", " ", str(txt)).strip()


def titulo(txt: str) -> str:
    """Title-case em PT-BR preservando preposições e siglas curtas."""
    txt = limpar(txt)
    if not txt:
        return ""
    minusc = {"de", "da", "do", "das", "dos", "e", "em", "na", "no", "a", "o"}
    palavras = []
    for i, p in enumerate(txt.lower().split(" ")):
        if i and p in minusc:
            palavras.append(p)
        elif re.fullmatch(r"(ltda|me|epp|eireli|s/a|sa|s\.a\.?|ms|br)", p) or \
                (i and re.fullmatch(r"(ii|iii|iv|vi|vii|viii|ix|xi|xii)", p)):
            palavras.append(p.upper())
        else:
            palavras.append(p[:1].upper() + p[1:])
    return " ".join(palavras)


def fmt_cnpj(basico: str, ordem: str, dv: str) -> str:
    d = f"{basico:0>8}{ordem:0>4}{dv:0>2}"
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def fmt_cep(cep: str) -> str:
    d = re.sub(r"\D", "", limpar(cep))
    return f"{d[:5]}-{d[5:]}" if len(d) == 8 else ""


def fmt_tel(ddd: str, num: str) -> str:
    ddd = re.sub(r"\D", "", limpar(ddd))
    num = re.sub(r"\D", "", limpar(num))
    if not num or set(num) == {"0"} or len(num) < 8:
        return ""
    ddd = ddd[-2:] if ddd else ""
    if len(num) == 9:
        corpo = f"{num[:5]}-{num[5:]}"
    else:
        num = num[-8:]
        corpo = f"{num[:4]}-{num[4:]}"
    return f"({ddd}) {corpo}" if ddd else corpo


def fmt_email(email: str) -> str:
    email = limpar(email).lower()
    return email if re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", email) else ""


def fmt_data(d: str) -> pd.Timestamp | None:
    d = re.sub(r"\D", "", limpar(d))
    if len(d) != 8 or d == "00000000":
        return None
    try:
        return pd.Timestamp(datetime.strptime(d, "%Y%m%d"))
    except ValueError:
        return None


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def jitter_deterministico(chave: str, raio_km: float) -> tuple[float, float]:
    """Deslocamento pseudo-aleatório estável (hash do CNPJ) dentro de um raio."""
    h = hashlib.sha256(chave.encode()).digest()
    ang = int.from_bytes(h[:4], "big") / 2**32 * 2 * math.pi
    dist = math.sqrt(int.from_bytes(h[4:8], "big") / 2**32) * raio_km
    dlat = (dist / 111.0) * math.cos(ang)
    dlon = (dist / 111.0) * math.sin(ang)
    return dlat, dlon


# --------------------------------------------------------------------------- #
# 3. Tabelas auxiliares
# --------------------------------------------------------------------------- #
def carregar_municipios_ms() -> pd.DataFrame:
    """79 municípios de MS com código IBGE, código SIAFI (usado pela RFB) e centroide."""
    r = SESSION.get(MUNICIPIOS_URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), dtype={"siafi_id": str, "codigo_ibge": str})
    df = df[df["codigo_uf"] == COD_UF_IBGE].copy()
    df["siafi_id"] = df["siafi_id"].str.zfill(4)
    df = df.rename(columns={"nome": "municipio", "latitude": "mun_lat", "longitude": "mun_lon"})
    if len(df) != 79:
        raise RuntimeError(f"Esperados 79 municípios de MS, obtidos {len(df)}.")
    return df[["codigo_ibge", "siafi_id", "municipio", "mun_lat", "mun_lon"]]


def ler_tabela_zip(path: Path, nomes: list[str]) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        with z.open(z.namelist()[0]) as f:
            return pd.read_csv(f, sep=";", header=None, names=nomes, dtype=str,
                               encoding="latin-1", quotechar='"')


# --------------------------------------------------------------------------- #
# 4. Filtro em streaming
# --------------------------------------------------------------------------- #
_RE_CNAES = re.compile("|".join(CNAES_ALVO))


def filtrar_estabelecimentos(path: Path, siafi_ms: set[str]) -> pd.DataFrame:
    partes = []
    with zipfile.ZipFile(path) as z:
        for membro in z.namelist():
            with z.open(membro) as f:
                leitor = pd.read_csv(
                    f, sep=";", header=None, names=COLS_ESTAB, dtype=str,
                    encoding="latin-1", quotechar='"', chunksize=500_000,
                    keep_default_na=False, on_bad_lines="warn",
                )
                for bloco in leitor:
                    bloco = bloco[(bloco["uf"] == UF_ALVO)
                                  & (bloco["situacao_cadastral"] == SITUACAO_ATIVA)]
                    if bloco.empty:
                        continue
                    alvo = bloco["cnae_principal"].isin(CNAES_ALVO.keys()) | \
                        bloco["cnae_secundaria"].str.contains(_RE_CNAES, na=False)
                    bloco = bloco[alvo & bloco["municipio_rfb"].isin(siafi_ms)]
                    if not bloco.empty:
                        partes.append(bloco)
    df = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=COLS_ESTAB)
    log(f"  {path.name}: {len(df):,} estabelecimentos-alvo em MS")
    return df


def filtrar_empresas(path: Path, basicos: set[str]) -> pd.DataFrame:
    partes = []
    with zipfile.ZipFile(path) as z:
        for membro in z.namelist():
            with z.open(membro) as f:
                leitor = pd.read_csv(
                    f, sep=";", header=None, names=COLS_EMPRESAS, dtype=str,
                    encoding="latin-1", quotechar='"', chunksize=1_000_000,
                    keep_default_na=False, on_bad_lines="warn",
                    usecols=["cnpj_basico", "razao_social", "porte"],
                )
                for bloco in leitor:
                    bloco = bloco[bloco["cnpj_basico"].isin(basicos)]
                    if not bloco.empty:
                        partes.append(bloco)
    df = pd.concat(partes, ignore_index=True) if partes else \
        pd.DataFrame(columns=["cnpj_basico", "razao_social", "porte"])
    log(f"  {path.name}: {len(df):,} empresas relacionadas")
    return df


# --------------------------------------------------------------------------- #
# 5. Geocodificação (Nominatim/OpenStreetMap) com cache persistente
# --------------------------------------------------------------------------- #
# Hierarquia: logradouro → bairro → centroide do município (+ jitter determinístico).
# Obs.: a BrasilAPI CEP v2 foi avaliada e descartada — para ~99% dos CEPs de MS
# ela devolve uma coordenada genérica (centro de Campo Grande).
_ultimo_req = [0.0]
_TIPOS_VIA = r"^(rua|r\.?|avenida|av\.?|travessa|tv\.?|alameda|al\.?|rodovia|rod\.?|estrada|est\.?|praca|praça|largo|quadra|linha|vila|beco|via)\s+"


def _nominatim(params: dict) -> dict | None:
    """Consulta o Nominatim respeitando a política de uso (1 req/s, User-Agent)."""
    params = {**params, "format": "jsonv2", "limit": 1, "countrycodes": "br",
              "addressdetails": 0}
    for tentativa in range(4):
        espera = NOMINATIM_INTERVALO - (time.time() - _ultimo_req[0])
        if espera > 0:
            time.sleep(espera)
        _ultimo_req[0] = time.time()
        try:
            r = SESSION.get(NOMINATIM_URL, params=params, timeout=30)
            if r.status_code in (429, 503):
                time.sleep(10 * (tentativa + 1))
                continue
            r.raise_for_status()
            res = r.json()
            if not res:
                return {"ok": False}
            return {"ok": True, "lat": float(res[0]["lat"]), "lon": float(res[0]["lon"]),
                    "tipo": res[0].get("addresstype") or res[0].get("type")}
        except (requests.RequestException, ValueError):
            time.sleep(2 ** tentativa)
    return None  # falha transitória: não grava no cache


def _chave(*partes: str) -> str:
    return "|".join(sem_acento(p) for p in partes)


def geocodificar(df: pd.DataFrame, orcamento_min: float | None) -> dict[str, dict]:
    """Geocodifica bairros e logradouros únicos, priorizando por nº de estabelecimentos."""
    cache: dict[str, dict] = json.loads(GEO_CACHE.read_text()) if GEO_CACHE.exists() else {}
    tarefas: list[tuple[int, str, dict]] = []
    bairros = df[df["bairro"] != ""].groupby(["bairro", "municipio"]).size()
    for (bairro, mun), n in bairros.items():
        k = "B|" + _chave(bairro, mun)
        if k not in cache:
            tarefas.append((n, k, {"q": f"{bairro}, {mun}, Mato Grosso do Sul"}))
    ruas = df[df["logradouro"].str.len() > 3].groupby(["logradouro", "municipio"]).size()
    tarefas_ruas = []
    for (rua, mun), n in ruas.items():
        k = "L|" + _chave(rua, mun)
        if k not in cache:
            tarefas_ruas.append((n, k, {"street": rua, "city": mun,
                                        "state": "Mato Grosso do Sul"}))
    # bairros primeiro (cobertura ampla), depois ruas por relevância
    tarefas = sorted(tarefas, key=lambda t: -t[0]) + sorted(tarefas_ruas, key=lambda t: -t[0])
    log(f"Geocodificação: {len(cache):,} chaves em cache; {len(tarefas):,} a consultar "
        f"({len(bairros):,} bairros e {len(ruas):,} logradouros únicos)")
    t0 = time.time()
    for i, (_, k, params) in enumerate(tarefas, 1):
        if orcamento_min is not None and time.time() - t0 > orcamento_min * 60:
            log(f"  orçamento de {orcamento_min:.0f} min atingido; {len(tarefas) - i + 1:,} "
                "consultas ficam para a próxima execução (cache incremental)")
            break
        res = _nominatim(params)
        if res is not None:
            cache[k] = res
        if i % 100 == 0 or i == len(tarefas):
            GEO_CACHE.write_text(json.dumps(cache, ensure_ascii=False))
            log(f"  {i:,}/{len(tarefas):,} consultas ({time.time() - t0:.0f}s)")
    GEO_CACHE.write_text(json.dumps(cache, ensure_ascii=False))
    return cache


def _valida(g: dict | None, mun_lat: float, mun_lon: float, raio_km: float) -> bool:
    if not g or not g.get("ok"):
        return False
    lat, lon = g["lat"], g["lon"]
    return (MS_BBOX["lat_min"] <= lat <= MS_BBOX["lat_max"]
            and MS_BBOX["lon_min"] <= lon <= MS_BBOX["lon_max"]
            and haversine_km(lat, lon, mun_lat, mun_lon) <= raio_km)


def atribuir_coordenadas(df: pd.DataFrame, geocache: dict[str, dict]) -> pd.DataFrame:
    lats, lons, precisao = [], [], []
    for row in df.itertuples(index=False):
        g_rua = geocache.get("L|" + _chave(row.logradouro, row.municipio))
        g_bai = geocache.get("B|" + _chave(row.bairro, row.municipio)) if row.bairro else None
        # bairro/rua genéricos que caem longe da sede são descartados
        if _valida(g_rua, row.mun_lat, row.mun_lon, 60):
            base, raio, nivel = (g_rua["lat"], g_rua["lon"]), 0.12, "Logradouro"
        elif _valida(g_bai, row.mun_lat, row.mun_lon, 40):
            base, raio, nivel = (g_bai["lat"], g_bai["lon"]), 0.35, "Bairro"
        else:
            base, raio, nivel = (row.mun_lat, row.mun_lon), 1.5, "Centroide do município"
        dlat, dlon = jitter_deterministico(row.cnpj, raio)
        lats.append(round(base[0] + dlat, 6))
        lons.append(round(base[1] + dlon, 6))
        precisao.append(nivel)
    df["latitude"], df["longitude"], df["precisao_geo"] = lats, lons, precisao
    return df


# --------------------------------------------------------------------------- #
# 6. Montagem final
# --------------------------------------------------------------------------- #
def classificar(row) -> tuple[str, str, str]:
    """Retorna (segmento, cnae_classificador, origem) — principal tem precedência."""
    if row.cnae_principal in CNAES_ALVO:
        return CNAES_ALVO[row.cnae_principal][0], row.cnae_principal, "Principal"
    secs = [c for c in limpar(row.cnae_secundaria).split(",") if c in CNAES_ALVO]
    melhor = min(secs, key=lambda c: CNAES_ALVO[c][1])
    return CNAES_ALVO[melhor][0], melhor, "Secundário"


def fmt_cnae(c: str) -> str:
    c = f"{c:0>7}"
    return f"{c[:4]}-{c[4]}/{c[5:]}"


def montar(estab: pd.DataFrame, empresas: pd.DataFrame, municipios: pd.DataFrame,
           cnaes: dict[str, str], geocode: bool,
           orcamento_min: float | None = None) -> pd.DataFrame:
    df = estab.merge(municipios, left_on="municipio_rfb", right_on="siafi_id", how="inner")
    df = df.merge(empresas.drop_duplicates("cnpj_basico"), on="cnpj_basico", how="left")

    df["cnpj"] = [fmt_cnpj(b, o, d) for b, o, d in
                  zip(df["cnpj_basico"], df["cnpj_ordem"], df["cnpj_dv"])]
    cls = [classificar(r) for r in df.itertuples(index=False)]
    df["segmento"] = [c[0] for c in cls]
    df["cnae_segmento"] = [fmt_cnae(c[1]) for c in cls]
    df["origem_cnae"] = [c[2] for c in cls]
    df["descricao_cnae"] = [
        cnaes.get(c, DESCRICOES_CNAE_PADRAO.get(c, "")) for c in df["cnae_principal"]
    ]
    df["cnae_principal_fmt"] = df["cnae_principal"].map(fmt_cnae)
    df["porte_empresa"] = df["porte"].map(PORTE_MAP).fillna("Não informado")
    df["razao_social"] = df["razao_social"].map(limpar).str.upper()
    df["nome_fantasia"] = df["nome_fantasia"].map(limpar).str.upper()
    df.loc[df["nome_fantasia"] == "", "nome_fantasia"] = df["razao_social"]
    df["situacao_cadastral"] = "ATIVA"
    df["matriz_filial"] = df["matriz_filial"].map({"1": "Matriz", "2": "Filial"}).fillna("")
    df["data_abertura"] = df["data_inicio_atividade"].map(fmt_data)
    df["tipo_logradouro"] = df["tipo_logradouro"].map(titulo)
    df["logradouro"] = df["logradouro"].map(titulo)
    tl = df["tipo_logradouro"]
    ja_tem_tipo = [l.lower().startswith(t.lower()) for t, l in zip(tl, df["logradouro"])]
    df["logradouro"] = [l if (not t or j) else f"{t} {l}"
                        for t, l, j in zip(tl, df["logradouro"], ja_tem_tipo)]
    df["numero"] = df["numero"].map(limpar).replace({"S/N": "S/N", "SN": "S/N", "0": "S/N"})
    df["complemento"] = df["complemento"].map(titulo)
    df["bairro"] = df["bairro"].map(titulo)
    df["cep"] = df["cep"].map(fmt_cep)
    df["telefone_1"] = [fmt_tel(d, n) for d, n in zip(df["ddd_1"], df["telefone_1"])]
    df["telefone_2"] = [fmt_tel(d, n) for d, n in zip(df["ddd_2"], df["telefone_2"])]
    df.loc[df["telefone_2"] == df["telefone_1"], "telefone_2"] = ""
    df["email"] = df["email"].map(fmt_email)

    def endereco(r) -> str:
        partes = [r.logradouro]
        if r.numero:
            partes[0] = f"{r.logradouro}, {r.numero}" if r.logradouro else r.numero
        if r.complemento:
            partes.append(r.complemento)
        if r.bairro:
            partes.append(r.bairro)
        partes.append(f"{r.municipio} - MS")
        if r.cep:
            partes.append(f"CEP {r.cep}")
        return " - ".join(p for p in partes if p)

    df["endereco_completo"] = [endereco(r) for r in df.itertuples(index=False)]

    if geocode:
        geocache = geocodificar(df, orcamento_min)
    elif GEO_CACHE.exists():
        geocache = json.loads(GEO_CACHE.read_text())
    else:
        geocache = {}
    df = atribuir_coordenadas(df, geocache)

    final = df.rename(columns={"cnae_principal_fmt": "cnae_principal_cod"})[[
        "cnpj", "razao_social", "nome_fantasia", "segmento", "origem_cnae",
        "cnae_segmento", "cnae_principal_cod", "descricao_cnae", "porte_empresa",
        "matriz_filial", "situacao_cadastral", "data_abertura", "municipio",
        "codigo_ibge", "bairro", "logradouro", "numero", "complemento", "cep",
        "endereco_completo", "telefone_1", "telefone_2", "email",
        "latitude", "longitude", "precisao_geo",
    ]].rename(columns={"cnae_principal_cod": "cnae_principal"})
    final = final.drop_duplicates("cnpj").sort_values(["municipio", "segmento", "nome_fantasia"])
    for col in ["segmento", "origem_cnae", "porte_empresa", "municipio", "precisao_geo",
                "matriz_filial", "situacao_cadastral", "cnae_segmento", "cnae_principal"]:
        final[col] = final[col].astype("category")
    return final.reset_index(drop=True)


def validar(df: pd.DataFrame) -> None:
    assert len(df) > 0, "Dataset vazio"
    assert df["cnpj"].is_unique, "CNPJs duplicados"
    assert df["cnpj"].str.fullmatch(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}").all(), "CNPJ mal formatado"
    assert (df["situacao_cadastral"] == "ATIVA").all()
    assert df["municipio"].nunique() <= 79
    assert df[["latitude", "longitude"]].notna().all().all(), "Coordenadas ausentes"
    assert df["latitude"].between(MS_BBOX["lat_min"], MS_BBOX["lat_max"]).all()
    assert df["longitude"].between(MS_BBOX["lon_min"], MS_BBOX["lon_max"]).all()
    assert set(df["segmento"].unique()) <= {v[0] for v in CNAES_ALVO.values()}


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mes", help="Mês de referência AAAA-MM (padrão: mais recente)")
    ap.add_argument("--skip-download", action="store_true", help="Usa arquivos já em data/raw")
    ap.add_argument("--no-geocode", action="store_true",
                    help="Não consulta o Nominatim (usa apenas o cache existente)")
    ap.add_argument("--geo-budget-min", type=float, default=None,
                    help="Tempo máximo (min) de consultas ao Nominatim nesta execução; "
                         "o restante fica no centroide e é completado em execuções futuras")
    ap.add_argument("--keep-raw", action="store_true", help="Mantém os .zip brutos ao final")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    if args.skip_download:
        meses = sorted({p.name[:7] for p in RAW_DIR.glob("????-??_Estabelecimentos*.zip")})
        mes = args.mes or (meses[-1] if meses else None)
        if not mes:
            sys.exit("Nenhum arquivo bruto em data/raw. Rode sem --skip-download.")
    else:
        mes = args.mes or descobrir_mes_mais_recente()
    log(f"Mês de referência RFB: {mes}")

    nomes_estab = [f"Estabelecimentos{i}.zip" for i in range(10)]
    nomes_emp = [f"Empresas{i}.zip" for i in range(10)]
    nomes = nomes_estab + nomes_emp + ["Cnaes.zip", "Municipios.zip"]
    if args.skip_download:
        caminhos = {n: RAW_DIR / f"{mes}_{n}" for n in nomes}
    else:
        caminhos = baixar_arquivos(mes, nomes)

    municipios = carregar_municipios_ms()
    siafi_ms = set(municipios["siafi_id"])
    # Confere nomes da tabela de municípios da própria RFB
    mun_rfb = ler_tabela_zip(caminhos["Municipios.zip"], ["codigo", "nome"])
    faltando = siafi_ms - set(mun_rfb["codigo"].str.zfill(4))
    if faltando:
        log(f"AVISO: códigos SIAFI ausentes na tabela da RFB: {sorted(faltando)}")
    cnaes_tab = ler_tabela_zip(caminhos["Cnaes.zip"], ["codigo", "descricao"])
    cnaes = dict(zip(cnaes_tab["codigo"].str.zfill(7), cnaes_tab["descricao"]))

    inter_estab = RAW_DIR / f"{mes}_filtrado_estabelecimentos.parquet"
    inter_emp = RAW_DIR / f"{mes}_filtrado_empresas.parquet"
    if args.skip_download and inter_estab.exists() and inter_emp.exists():
        log("Usando resultado filtrado intermediário em cache.")
        estab, empresas = pd.read_parquet(inter_estab), pd.read_parquet(inter_emp)
    else:
        log("Filtrando Estabelecimentos (UF=MS, ATIVA, CNAEs-alvo)…")
        with ThreadPoolExecutor(4) as pool:
            estab = pd.concat(pool.map(lambda n: filtrar_estabelecimentos(caminhos[n], siafi_ms),
                                       nomes_estab), ignore_index=True)
        log(f"Total: {len(estab):,} estabelecimentos")

        log("Cruzando com Empresas (razão social e porte)…")
        basicos = set(estab["cnpj_basico"])
        with ThreadPoolExecutor(4) as pool:
            empresas = pd.concat(pool.map(lambda n: filtrar_empresas(caminhos[n], basicos),
                                          nomes_emp), ignore_index=True)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        estab.to_parquet(inter_estab, index=False)
        empresas.to_parquet(inter_emp, index=False)

    df = montar(estab, empresas, municipios, cnaes, geocode=not args.no_geocode,
                orcamento_min=args.geo_budget_min)
    validar(df)
    df.to_parquet(OUT_PARQUET, index=False, compression="zstd")

    meta = {
        "fonte": "Receita Federal do Brasil — Dados Abertos do CNPJ",
        "mes_referencia": mes,
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "registros": int(len(df)),
        "municipios_com_registros": int(df["municipio"].nunique()),
        "por_segmento": df["segmento"].value_counts().to_dict(),
        "por_origem_cnae": df["origem_cnae"].value_counts().to_dict(),
        "precisao_geo": df["precisao_geo"].value_counts().to_dict(),
        "com_telefone": int((df["telefone_1"] != "").sum()),
        "com_email": int((df["email"] != "").sum()),
    }
    METADATA.write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    log(f"✔ {OUT_PARQUET.relative_to(ROOT)} gerado: {len(df):,} registros "
        f"({OUT_PARQUET.stat().st_size / 2**20:.1f} MB) em {time.time() - t0:.0f}s")
    log(json.dumps(meta, ensure_ascii=False, indent=2))

    if not args.keep_raw and not args.skip_download:
        for p in caminhos.values():
            p.unlink(missing_ok=True)
        inter_estab.unlink(missing_ok=True)
        inter_emp.unlink(missing_ok=True)
        log("Arquivos brutos removidos (use --keep-raw para mantê-los).")


if __name__ == "__main__":
    main()
