"""Carregamento e filtragem do dataset consolidado (data/estabelecimentos_ms.parquet)."""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PARQUET_PATH = DATA_DIR / "estabelecimentos_ms.parquet"
METADATA_PATH = DATA_DIR / "metadata.json"

SEGMENTOS = [
    "Supermercado",
    "Hipermercado",
    "Atacadista / Atacarejo",
    "Minimercado / Mercearia",
]
CORES_SEGMENTO = {
    "Supermercado": "#004B87",
    "Hipermercado": "#004B87",
    "Atacadista / Atacarejo": "#007A33",
    "Minimercado / Mercearia": "#F2A900",
}
PORTES = {
    "ME": "Microempresa - ME",
    "EPP": "Empresa de Pequeno Porte - EPP",
    "Demais": "Médio/Grande Porte - Demais",
    "Não informado": "Não informado",
}
POLOS = [
    "Campo Grande", "Dourados", "Três Lagoas", "Corumbá", "Ponta Porã",
    "Naviraí", "Nova Andradina", "Aquidauana", "Sidrolândia", "Paranaíba",
    "Maracaju", "Coxim", "Amambai", "Rio Brilhante", "Chapadão do Sul",
]


def normalizar(txt: str) -> str:
    """Minúsculas, sem acentos e sem pontuação — usado na busca textual."""
    txt = unicodedata.normalize("NFKD", str(txt))
    txt = "".join(c for c in txt if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9 ]+", " ", txt)


@st.cache_data(show_spinner="Carregando base de estabelecimentos…")
def carregar_dados(_mtime: float | None = None) -> pd.DataFrame:
    """Lê o parquet e pré-computa colunas auxiliares (cacheado por mtime do arquivo)."""
    df = pd.read_parquet(PARQUET_PATH)
    for c in ["telefone_1", "telefone_2", "email", "bairro", "complemento", "numero"]:
        df[c] = df[c].fillna("").astype(str)
    df["tem_telefone"] = (df["telefone_1"] != "") | (df["telefone_2"] != "")
    df["tem_email"] = df["email"] != ""
    df["_busca"] = (
        df["nome_fantasia"].astype(str) + " " + df["razao_social"].astype(str) + " "
        + df["bairro"] + " " + df["cnpj"].str.replace(r"\D", "", regex=True)
    ).map(normalizar)
    return df


def dados() -> pd.DataFrame:
    mtime = PARQUET_PATH.stat().st_mtime if PARQUET_PATH.exists() else None
    return carregar_dados(mtime)


@st.cache_data
def carregar_metadata(_mtime: float | None = None) -> dict:
    if METADATA_PATH.exists():
        return json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    return {}


def metadata() -> dict:
    return carregar_metadata(METADATA_PATH.stat().st_mtime if METADATA_PATH.exists() else None)


@dataclass
class Filtros:
    municipios: list[str] = field(default_factory=list)  # vazio = todos
    segmentos: list[str] = field(default_factory=list)
    portes: list[str] = field(default_factory=list)
    so_telefone: bool = False
    so_email: bool = False
    incluir_secundario: bool = False
    busca: str = ""


def aplicar_filtros(df: pd.DataFrame, f: Filtros) -> pd.DataFrame:
    m = pd.Series(True, index=df.index)
    if not f.incluir_secundario:
        m &= df["origem_cnae"] == "Principal"
    if f.municipios:
        m &= df["municipio"].isin(f.municipios)
    if f.segmentos:
        m &= df["segmento"].isin(f.segmentos)
    if f.portes:
        m &= df["porte_empresa"].isin(f.portes)
    if f.so_telefone:
        m &= df["tem_telefone"]
    if f.so_email:
        m &= df["tem_email"]
    busca = f.busca.strip()
    if busca:
        digitos = re.sub(r"\D", "", busca)
        if len(digitos) >= 8 and re.fullmatch(r"[\d./\-\s]+", busca):
            # CNPJ (completo ou parcial), com ou sem máscara
            m &= df["cnpj"].str.replace(r"\D", "", regex=True).str.contains(digitos, regex=False)
        else:
            # todos os termos devem aparecer (AND), sem diferenciar acentos/maiúsculas
            for t in normalizar(busca).split():
                m &= df["_busca"].str.contains(t, regex=False)
    return df[m]
