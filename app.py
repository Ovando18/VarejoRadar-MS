"""VarejoRadar-MS — Inteligência comercial de supermercados e atacadistas de Mato Grosso do Sul."""
from __future__ import annotations

import html
import json
from datetime import datetime
from urllib.parse import quote_plus

import folium
import pandas as pd
import streamlit as st
from branca.element import MacroElement, Template
from folium.plugins import FastMarkerCluster
from streamlit_folium import st_folium

from utils.data_loader import (
    CORES_SEGMENTO, PARQUET_PATH, POLOS, PORTES, SEGMENTOS, Filtros,
    aplicar_filtros, dados, metadata,
)
from utils.excel_export import gerar_excel

st.set_page_config(
    page_title="VarejoRadar-MS | Supermercados e Atacadistas",
    page_icon="🛒",
    layout="wide",
)

AZUL, VERDE, OURO = "#004B87", "#007A33", "#F2A900"
MAX_PONTOS_MAPA = 15000  # acima disso, amostra priorizada

# --------------------------------------------------------------------------- #
# CSS — design system MS
# --------------------------------------------------------------------------- #
st.markdown(
    f"""
<style>
  :root {{ --azul:{AZUL}; --verde:{VERDE}; --ouro:{OURO}; --borda:#E0E6ED; --texto:#1F2A37; --suave:#5B6B7F; }}
  .stApp {{ background:#F4F6F9; }}
  .block-container {{ padding-top:1.4rem; padding-bottom:2rem; max-width:1500px; }}
  [data-testid="stSidebar"] {{ background:#FFFFFF; border-right:1px solid var(--borda); }}
  [data-testid="stSidebar"] h2 {{ color:var(--azul); font-size:1.05rem; }}
  .vr-header {{ background:linear-gradient(100deg,{AZUL} 0%,#0A5C9E 70%,#0E6AAF 100%);
      border-radius:12px; padding:20px 26px; color:#fff; margin-bottom:18px;
      border-bottom:4px solid {OURO}; display:flex; justify-content:space-between; align-items:center; gap:16px; flex-wrap:wrap; }}
  .vr-header h1 {{ color:#fff; font-size:1.65rem; margin:0; font-weight:700; letter-spacing:-.01em; padding:0; }}
  .vr-header p {{ margin:4px 0 0; opacity:.88; font-size:.93rem; }}
  .vr-fonte {{ background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.25);
      border-radius:8px; padding:8px 12px; font-size:.8rem; line-height:1.35; }}
  .kpi {{ background:#fff; border:1px solid var(--borda); border-radius:10px; padding:16px 18px;
      box-shadow:0 2px 6px rgba(0,0,0,.04); min-height:128px; border-top:4px solid var(--azul); }}
  .kpi.verde {{ border-top-color:var(--verde); }}
  .kpi.ouro {{ border-top-color:var(--ouro); }}
  .kpi.destaque {{ border-top-color:var(--ouro); box-shadow:0 2px 10px rgba(242,169,0,.18); }}
  .kpi .rot {{ color:var(--suave); font-size:.78rem; text-transform:uppercase; letter-spacing:.05em; font-weight:600; }}
  .kpi .val {{ color:var(--texto); font-size:2rem; font-weight:700; line-height:1.15; margin-top:4px; font-variant-numeric:tabular-nums; }}
  .kpi .sub {{ color:var(--suave); font-size:.82rem; margin-top:2px; }}
  .kpi .sub b {{ color:var(--verde); }}
  .card {{ background:#fff; border:1px solid var(--borda); border-radius:10px; padding:16px 18px;
      box-shadow:0 2px 6px rgba(0,0,0,.04); }}
  .sec {{ color:var(--azul); font-weight:700; font-size:1.08rem; margin:18px 0 8px; display:flex; align-items:center; gap:8px; }}
  .sec:before {{ content:""; width:4px; height:18px; background:var(--ouro); border-radius:2px; display:inline-block; }}
  .badge {{ display:inline-block; padding:2px 9px; border-radius:999px; font-size:.74rem; font-weight:600; color:#fff; }}
  .det h3 {{ margin:6px 0 2px; color:var(--texto); font-size:1.15rem; padding:0; }}
  .det .rs {{ color:var(--suave); font-size:.86rem; margin-bottom:10px; }}
  .det table {{ width:100%; border-collapse:collapse; font-size:.86rem; }}
  .det table, .det tr, .det td {{ border-left:none !important; border-right:none !important; border-top:none !important; }}
  .det td {{ padding:6px 0; border-bottom:1px solid #EEF2F6 !important; vertical-align:top; }}
  .det td:first-child {{ color:var(--suave); width:34%; padding-right:8px; }}
  .det a {{ color:var(--azul); font-weight:600; text-decoration:none; }}
  .vazio {{ color:var(--suave); text-align:center; padding:36px 10px; font-size:.9rem; }}
  .legenda {{ display:flex; gap:16px; flex-wrap:wrap; font-size:.82rem; color:var(--suave); margin:2px 0 8px; }}
  .legenda span i {{ display:inline-block; width:11px; height:11px; border-radius:50%; margin-right:6px; vertical-align:-1px; border:2px solid #fff; box-shadow:0 0 0 1px rgba(0,0,0,.2); }}
  div.stDownloadButton > button {{ background:{VERDE}; color:#fff; border:0; font-weight:600; padding:.55rem 1.1rem; }}
  div.stDownloadButton > button:hover {{ background:#00652A; color:#fff; border:0; }}
  [data-testid="stDataFrame"] {{ border:1px solid var(--borda); border-radius:8px; }}
  footer {{ visibility:hidden; }}
</style>
""",
    unsafe_allow_html=True,
)


def fmt_int(n: int | float) -> str:
    return f"{int(n):,}".replace(",", ".")


def pct(a: int, b: int) -> str:
    return f"{(100 * a / b):.0f}%" if b else "—"


def esc(v) -> str:
    return html.escape("" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))


# --------------------------------------------------------------------------- #
# Dados
# --------------------------------------------------------------------------- #
if not PARQUET_PATH.exists():
    st.error("Base `data/estabelecimentos_ms.parquet` não encontrada. "
             "Execute `python scripts/fetch_data.py` para gerá-la.")
    st.stop()

base = dados()
meta = metadata()

# --------------------------------------------------------------------------- #
# Sidebar — filtros
# --------------------------------------------------------------------------- #
todos_municipios = sorted(base["municipio"].astype(str).unique())
polos_existentes = [p for p in POLOS if p in todos_municipios]
opcoes_municipio = polos_existentes + [m for m in todos_municipios if m not in polos_existentes]

with st.sidebar:
    st.markdown("## 🔎 Filtros")

    if "f_mun" not in st.session_state:
        st.session_state.f_mun = []
    c1, c2 = st.columns(2)
    if c1.button("⭐ Polos", width="stretch",
                 help="Seleciona os principais polos econômicos de MS"):
        st.session_state.f_mun = polos_existentes[:10]
    if c2.button("Limpar", width="stretch", help="Limpa a seleção (todos os municípios)"):
        st.session_state.f_mun = []
    municipios = st.multiselect(
        "Município(s)", opcoes_municipio, key="f_mun",
        placeholder="Todos os Municípios",
        help="Deixe vazio para considerar os 79 municípios. Polos aparecem primeiro na lista.",
    )

    segmentos = st.multiselect("Segmento / Tipo de Estabelecimento", SEGMENTOS,
                               placeholder="Todos os segmentos")
    portes_sel = st.multiselect(
        "Porte da Empresa", list(PORTES), format_func=PORTES.get, placeholder="Todos os portes",
    )

    st.markdown("**Contato disponível**")
    so_tel = st.checkbox("📞 Apenas com telefone cadastrado")
    so_email = st.checkbox("✉️ Apenas com e-mail cadastrado")

    busca = st.text_input("Busca livre", placeholder="Nome, razão social, CNPJ ou bairro")

    with st.expander("Opções avançadas"):
        incluir_sec = st.checkbox(
            "Incluir CNAE secundário",
            help="Inclui estabelecimentos cuja atividade principal é outra (ex.: padaria, "
                 "posto, açougue), mas que têm um dos CNAEs-alvo como atividade secundária.",
        )

    st.divider()
    if meta:
        ref = meta.get("mes_referencia", "")
        try:
            ref = datetime.strptime(ref, "%Y-%m").strftime("%m/%Y")
        except ValueError:
            pass
        st.caption(
            f"**Fonte:** Receita Federal do Brasil — Dados Abertos do CNPJ "
            f"(referência {ref}). Situação cadastral: ATIVA. "
            f"Geocodificação: © OpenStreetMap/Nominatim (logradouro ou bairro) "
            f"ou centroide do município."
        )

filtros = Filtros(municipios=municipios, segmentos=segmentos, portes=portes_sel,
                  so_telefone=so_tel, so_email=so_email, incluir_secundario=incluir_sec,
                  busca=busca)
df = aplicar_filtros(base, filtros)

# --------------------------------------------------------------------------- #
# Cabeçalho
# --------------------------------------------------------------------------- #
ref_txt = ""
if meta.get("mes_referencia"):
    try:
        ref_txt = datetime.strptime(meta["mes_referencia"], "%Y-%m").strftime("%m/%Y")
    except ValueError:
        ref_txt = meta["mes_referencia"]
st.markdown(
    f"""
<div class="vr-header">
  <div>
    <h1>🛒 VarejoRadar-MS</h1>
    <p>Inteligência comercial de supermercados, hipermercados, minimercados e atacadistas · Mato Grosso do Sul</p>
  </div>
  <div class="vr-fonte">Base oficial RFB · CNPJs ativos<br><b>Referência {esc(ref_txt) or '—'}</b> · {fmt_int(len(base))} estabelecimentos</div>
</div>
""",
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------- #
# KPIs
# --------------------------------------------------------------------------- #
total = len(df)
n_super = int(df["segmento"].isin(["Supermercado", "Hipermercado"]).sum())
n_hiper = int((df["segmento"] == "Hipermercado").sum())
n_atac = int((df["segmento"] == "Atacadista / Atacarejo").sum())
n_mini = int((df["segmento"] == "Minimercado / Mercearia").sum())
n_mun = int(df["municipio"].nunique())
n_tel = int(df["tem_telefone"].sum())
n_mail = int(df["tem_email"].sum())

k1, k2, k3, k4 = st.columns(4)
k1.markdown(f"""<div class="kpi destaque"><div class="rot">Estabelecimentos filtrados</div>
<div class="val">{fmt_int(total)}</div>
<div class="sub">{fmt_int(n_mini)} minimercados/mercearias</div></div>""", unsafe_allow_html=True)
k2.markdown(f"""<div class="kpi"><div class="rot">Super & Hipermercados</div>
<div class="val">{fmt_int(n_super)}</div>
<div class="sub">{fmt_int(n_hiper)} hipermercados · {pct(n_super, total)} do total</div></div>""",
            unsafe_allow_html=True)
k3.markdown(f"""<div class="kpi verde"><div class="rot">Atacadistas & Atacarejos</div>
<div class="val">{fmt_int(n_atac)}</div>
<div class="sub">{pct(n_atac, total)} do total filtrado</div></div>""", unsafe_allow_html=True)
k4.markdown(f"""<div class="kpi ouro"><div class="rot">Municípios abrangidos</div>
<div class="val">{n_mun} <span style="font-size:1rem;color:#5B6B7F;font-weight:500">/ 79</span></div>
<div class="sub">📞 <b>{pct(n_tel, total)}</b> c/ telefone · ✉️ <b>{pct(n_mail, total)}</b> c/ e-mail</div></div>""",
            unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# Mapa
# --------------------------------------------------------------------------- #
LEGENDA_JS = Template("""
{% macro script(this, kwargs) %}
var legenda = L.control({position: 'bottomright'});
legenda.onAdd = function () {
  var d = L.DomUtil.create('div');
  d.style.cssText = 'background:#fff;padding:8px 12px;border-radius:8px;border:1px solid #E0E6ED;' +
    'box-shadow:0 2px 6px rgba(0,0,0,.08);font:12px/1.6 sans-serif;color:#1F2A37';
  d.innerHTML = '<b style="color:#004B87">Segmento</b><br>' +
    '<span style="color:#004B87;font-size:16px;line-height:1">●</span> Supermercado / Hipermercado<br>' +
    '<span style="color:#007A33;font-size:16px;line-height:1">●</span> Atacadista / Atacarejo<br>' +
    '<span style="color:#F2A900;font-size:16px;line-height:1">●</span> Minimercado / Mercearia';
  return d;
};
legenda.addTo({{ this._parent.get_name() }});
{% endmacro %}
""")


SEG_IDX = {s: i for i, s in enumerate(SEGMENTOS)}

# Marcadores criados no navegador a partir de JSON compacto (muito mais leve que
# milhares de objetos folium.Marker). Popup em HTML montado no próprio callback.
CALLBACK_JS = """
function (row) {
  var SEG = %(segs)s, COR = %(cores)s;
  function e(t) { return String(t || '').replace(/[&<>"']/g, function (c) {
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  var seg = SEG[row[2]], cor = COR[row[2]], big = (row[2] === 1 || row[2] === 2);
  var sz = big ? 16 : 13;
  var icon = L.divIcon({className: '', iconSize: [sz, sz], iconAnchor: [sz / 2, sz / 2],
    html: '<div style="width:' + sz + 'px;height:' + sz + 'px;border-radius:50%%;background:' + cor +
          ';border:2px solid #fff;box-shadow:0 0 0 1px rgba(0,0,0,.35)"></div>'});
  var m = L.marker(new L.LatLng(row[0], row[1]), {icon: icon});
  var q = row[9] ? encodeURIComponent(row[9]) : (row[0] + ',' + row[1]);
  m.bindTooltip(e(row[3]) + ' · ' + e(seg));
  m.bindPopup(
    '<div style="font-family:sans-serif;font-size:12.5px;line-height:1.45;color:#1F2A37;min-width:250px">' +
    '<span style="background:' + cor + ';color:#fff;border-radius:999px;padding:1px 8px;font-size:11px;font-weight:600">' + e(seg) + '</span>' +
    '<div style="font-size:14px;font-weight:700;margin:6px 0 1px;color:#004B87">' + e(row[3]) + '</div>' +
    '<div style="color:#5B6B7F;font-size:11.5px;margin-bottom:6px">' + e(row[4]) + '</div>' +
    '<b>CNPJ:</b> ' + e(row[5]) + '<br><b>Endereço:</b> ' + e(row[6]) + '<br>' +
    '<b>Telefone:</b> ' + (e(row[7]) || '—') + '<br><b>E-mail:</b> ' + (e(row[8]) || '—') + '<br>' +
    '<a href="https://www.google.com/maps/search/?api=1&query=' + q + '" target="_blank" rel="noopener" ' +
    'style="display:inline-block;margin-top:7px;color:#fff;background:#007A33;padding:3px 10px;border-radius:6px;text-decoration:none;font-weight:600">' +
    '📍 Abrir no Google Maps</a></div>', {maxWidth: 340});
  return m;
};
""" % {"segs": json.dumps(SEGMENTOS, ensure_ascii=False),
       "cores": json.dumps([CORES_SEGMENTO[s] for s in SEGMENTOS])}


def link_gmaps(lat: float, lon: float, endereco: str, precisao: str) -> str:
    """Coordenada se for de logradouro; senão, busca textual do endereço (mais precisa)."""
    if precisao == "Logradouro":
        return f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
    return "https://www.google.com/maps/search/?api=1&query=" + quote_plus(endereco)


# Clusters na identidade MS: azul institucional com anel ouro
CLUSTER_JS = """
function (cluster) {
  var n = cluster.getChildCount();
  var d = n < 50 ? 34 : (n < 500 ? 42 : 52);
  var txt = n >= 1000 ? (n / 1000).toFixed(1).replace('.', ',') + 'k' : n;
  return L.divIcon({
    className: '', iconSize: L.point(d, d),
    html: '<div style="width:' + d + 'px;height:' + d + 'px;border-radius:50%;display:flex;' +
          'align-items:center;justify-content:center;background:rgba(0,75,135,.88);color:#fff;' +
          'font:600 12px sans-serif;border:3px solid rgba(242,169,0,.85);' +
          'box-shadow:0 1px 4px rgba(0,0,0,.25)">' + txt + '</div>'
  });
}
"""


@st.cache_data(show_spinner=False, max_entries=24)
def preparar_pontos(pontos: pd.DataFrame) -> tuple[list[list], list[list[float]]]:
    """Linhas compactas para o JS + limites do mapa (cacheado por conteúdo)."""
    if len(pontos):
        sw = [float(pontos["latitude"].min()), float(pontos["longitude"].min())]
        ne = [float(pontos["latitude"].max()), float(pontos["longitude"].max())]
    else:
        sw, ne = [-24.1, -58.2], [-17.2, -50.9]
    tels = [" · ".join(t for t in par if t)
            for par in zip(pontos["telefone_1"], pontos["telefone_2"])]
    consulta = ["" if pr == "Logradouro" else end
                for pr, end in zip(pontos["precisao_geo"], pontos["endereco_completo"])]
    linhas = [
        [round(float(la), 6), round(float(lo), 6), SEG_IDX.get(str(sg), 0), nf, rs, cn, end,
         tl, em, cq]
        for la, lo, sg, nf, rs, cn, end, tl, em, cq in zip(
            pontos["latitude"], pontos["longitude"], pontos["segmento"],
            pontos["nome_fantasia"].astype(str), pontos["razao_social"].astype(str),
            pontos["cnpj"], pontos["endereco_completo"], tels, pontos["email"], consulta)
    ]
    return linhas, [sw, ne]


def construir_mapa(pontos: pd.DataFrame) -> folium.Map:
    """Monta um folium.Map novo a cada execução.

    Não usar st.cache_resource aqui: o st_folium altera os ``_id`` internos dos
    elementos ao serializar o mapa, e reaproveitar o mesmo objeto quebra as
    referências JS na execução seguinte (mapa em branco).
    """
    linhas, (sw, ne) = preparar_pontos(pontos)
    m = folium.Map(location=[(sw[0] + ne[0]) / 2, (sw[1] + ne[1]) / 2], zoom_start=6,
                   tiles=None, control_scale=True)
    # Tiles sem chave de API (CartoDB passou a exigir chave no folium >= 0.19)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/"
              "World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ", name="Mapa claro",
        max_native_zoom=16, max_zoom=19,
    ).add_to(m)
    m.fit_bounds([sw, ne], padding=(20, 20), max_zoom=15)
    FastMarkerCluster(
        linhas, callback=CALLBACK_JS, name="Estabelecimentos",
        options={"showCoverageOnHover": False, "maxClusterRadius": 45,
                 "disableClusteringAtZoom": 17, "chunkedLoading": True},
        icon_create_function=CLUSTER_JS,
    ).add_to(m)
    leg = MacroElement()
    leg._template = LEGENDA_JS
    m.add_child(leg)
    return m


st.markdown('<div class="sec">Mapa georreferenciado</div>', unsafe_allow_html=True)

prioridade = {"Hipermercado": 0, "Atacadista / Atacarejo": 1, "Supermercado": 2,
              "Minimercado / Mercearia": 3}
pontos = df
if len(df) > MAX_PONTOS_MAPA:
    pontos = (df.assign(_p=df["segmento"].astype(str).map(prioridade))
              .sort_values(["_p", "cnpj"]).head(MAX_PONTOS_MAPA).drop(columns="_p"))
    st.info(
        f"O mapa exibe {fmt_int(MAX_PONTOS_MAPA)} de {fmt_int(len(df))} estabelecimentos "
        "(priorizando hipermercados, atacadistas e supermercados). Refine os filtros para ver "
        "todos os pontos — a tabela e a exportação contemplam **todos** os registros.",
        icon="ℹ️",
    )

col_mapa, col_det = st.columns([2.3, 1], gap="medium")
colunas_mapa = ["cnpj", "nome_fantasia", "razao_social", "segmento", "endereco_completo",
                "telefone_1", "telefone_2", "email", "latitude", "longitude", "precisao_geo"]
with col_mapa:
    mapa = construir_mapa(pontos[colunas_mapa].reset_index(drop=True))
    retorno = st_folium(mapa, height=580, width=None,
                        returned_objects=["last_object_clicked"], key="mapa_ms")

selecionado = None
clique = (retorno or {}).get("last_object_clicked")
if clique and len(pontos):
    d2 = (pontos["latitude"] - clique["lat"]) ** 2 + (pontos["longitude"] - clique["lng"]) ** 2
    idx = d2.idxmin()
    if d2.loc[idx] < 1e-8:
        selecionado = pontos.loc[idx]

with col_det:
    st.markdown('<div class="sec" style="margin-top:0">Estabelecimento selecionado</div>',
                unsafe_allow_html=True)
    if selecionado is None:
        st.markdown('<div class="card vazio">🖱️ Clique em um ponto do mapa para ver aqui '
                    'os dados cadastrais completos, prontos para cópia.</div>',
                    unsafe_allow_html=True)
    else:
        s = selecionado
        cor = CORES_SEGMENTO.get(s["segmento"], AZUL)
        tels = " · ".join(t for t in [s["telefone_1"], s["telefone_2"]] if t) or "—"
        abertura = pd.to_datetime(s["data_abertura"]).strftime("%d/%m/%Y") \
            if pd.notna(s["data_abertura"]) else "—"
        gmaps = link_gmaps(s["latitude"], s["longitude"], s["endereco_completo"], s["precisao_geo"])
        st.markdown(
            f"""<div class="card det">
<span class="badge" style="background:{cor}">{esc(s['segmento'])}</span>
<h3>{esc(s['nome_fantasia'])}</h3><div class="rs">{esc(s['razao_social'])}</div>
<table>
<tr><td>CNPJ</td><td><b>{esc(s['cnpj'])}</b> · {esc(s['matriz_filial'])}</td></tr>
<tr><td>CNAE principal</td><td>{esc(s['cnae_principal'])} — {esc(s['descricao_cnae'])}</td></tr>
<tr><td>Porte</td><td>{esc(PORTES.get(str(s['porte_empresa']), s['porte_empresa']))}</td></tr>
<tr><td>Abertura</td><td>{abertura}</td></tr>
<tr><td>Endereço</td><td>{esc(s['endereco_completo'])}</td></tr>
<tr><td>Telefone(s)</td><td>{esc(tels)}</td></tr>
<tr><td>E-mail</td><td>{esc(s['email']) or '—'}</td></tr>
<tr><td>Localização</td><td><a href="{gmaps}" target="_blank">📍 Google Maps</a>
 <span style="color:#5B6B7F;font-size:.78rem">({esc(s['precisao_geo'])})</span></td></tr>
</table></div>""",
            unsafe_allow_html=True,
        )
        texto = "\n".join([
            f"{s['nome_fantasia']}",
            f"Razão Social: {s['razao_social']}",
            f"CNPJ: {s['cnpj']}",
            f"Segmento: {s['segmento']}",
            f"Endereço: {s['endereco_completo']}",
            f"Telefone(s): {tels}",
            f"E-mail: {s['email'] or '—'}",
        ])
        st.caption("Copiar dados:")
        st.code(texto, language=None)

# --------------------------------------------------------------------------- #
# Tabela + exportação
# --------------------------------------------------------------------------- #
st.markdown('<div class="sec">Listagem de estabelecimentos</div>', unsafe_allow_html=True)

tabela = df[["nome_fantasia", "razao_social", "cnpj", "segmento", "municipio", "bairro",
             "endereco_completo", "telefone_1", "email", "porte_empresa", "data_abertura"]]

partes_filtro = []
if municipios:
    partes_filtro.append("Municípios: " + ", ".join(municipios))
if segmentos:
    partes_filtro.append("Segmentos: " + ", ".join(segmentos))
if portes_sel:
    partes_filtro.append("Porte: " + ", ".join(portes_sel))
if so_tel:
    partes_filtro.append("com telefone")
if so_email:
    partes_filtro.append("com e-mail")
if busca:
    partes_filtro.append(f'busca "{busca}"')
if incluir_sec:
    partes_filtro.append("inclui CNAE secundário")

c_info, c_btn = st.columns([3, 1.2])
c_info.caption(f"{fmt_int(len(df))} registros · " + ("; ".join(partes_filtro) or "sem filtros aplicados"))


@st.cache_data(show_spinner=False, max_entries=8)
def excel_bytes(_df: pd.DataFrame, chave: tuple, descricao: str) -> bytes:
    return gerar_excel(_df, descricao)


chave = (tuple(municipios), tuple(segmentos), tuple(portes_sel), so_tel, so_email,
         busca, incluir_sec, PARQUET_PATH.stat().st_mtime)
with c_btn:
    st.download_button(
        "📥 Exportar Dados Filtrados (.xlsx)",
        data=excel_bytes(df, chave, "; ".join(partes_filtro)),
        file_name=f"varejoradar_ms_{datetime.now():%Y%m%d_%H%M}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch", disabled=len(df) == 0,
    )

st.dataframe(
    tabela,
    hide_index=True,
    width="stretch",
    height=460,
    column_config={
        "nome_fantasia": st.column_config.TextColumn("Nome Fantasia", width="medium"),
        "razao_social": st.column_config.TextColumn("Razão Social", width="medium"),
        "cnpj": st.column_config.TextColumn("CNPJ", width="small"),
        "segmento": st.column_config.TextColumn("Segmento", width="small"),
        "municipio": st.column_config.TextColumn("Município", width="small"),
        "bairro": st.column_config.TextColumn("Bairro", width="small"),
        "endereco_completo": st.column_config.TextColumn("Endereço", width="large"),
        "telefone_1": st.column_config.TextColumn("Telefone", width="small"),
        "email": st.column_config.TextColumn("E-mail", width="medium"),
        "porte_empresa": st.column_config.TextColumn("Porte", width="small"),
        "data_abertura": st.column_config.DateColumn("Abertura", format="DD/MM/YYYY"),
    },
)

# --------------------------------------------------------------------------- #
# Distribuição
# --------------------------------------------------------------------------- #
if len(df):
    st.markdown('<div class="sec">Distribuição</div>', unsafe_allow_html=True)
    g1, g2 = st.columns([1.6, 1], gap="medium")
    with g1:
        top = (df.assign(municipio=df["municipio"].astype(str), segmento=df["segmento"].astype(str))
               .pivot_table(index="municipio", columns="segmento", values="cnpj",
                            aggfunc="count", fill_value=0))
        top = top.loc[top.sum(axis=1).sort_values(ascending=False).index[:15]]
        ordem = [s for s in ["Hipermercado", "Supermercado", "Atacadista / Atacarejo",
                             "Minimercado / Mercearia"] if s in top.columns]
        top = top[ordem]
        cores = {"Hipermercado": "#002F57", "Supermercado": AZUL,
                 "Atacadista / Atacarejo": VERDE, "Minimercado / Mercearia": OURO}
        st.caption("Top 15 municípios por número de estabelecimentos")
        st.bar_chart(top, horizontal=True, color=[cores[c] for c in ordem], height=420,
                     x_label="Estabelecimentos", y_label="")
    with g2:
        st.caption("Por porte da empresa")
        porte = (df["porte_empresa"].astype(str).map(lambda p: PORTES.get(p, p))
                 .value_counts().rename("Estabelecimentos"))
        st.bar_chart(porte, color=AZUL, height=200, x_label="", y_label="")
        st.caption("Contato disponível")
        contato = pd.Series({"Com telefone": n_tel, "Com e-mail": n_mail,
                             "Tel. e e-mail": int((df["tem_telefone"] & df["tem_email"]).sum())},
                            name="Estabelecimentos")
        st.bar_chart(contato, color=VERDE, height=170, x_label="", y_label="")

st.caption(
    "Dados públicos da Receita Federal do Brasil (Dados Abertos do CNPJ), tratados pelo "
    "VarejoRadar-MS. Pontos com precisão “Centroide do município” têm localização aproximada. "
    "Uso de contatos sujeito à LGPD (Lei 13.709/2018)."
)
