"""Exportação profissional para Excel (.xlsx) com xlsxwriter."""
from __future__ import annotations

import io
from datetime import datetime

import pandas as pd

AZUL_MS = "#004B87"
VERDE_MS = "#007A33"
OURO_MS = "#F2A900"

COLUNAS_EXPORT = {
    "nome_fantasia": "Nome Fantasia",
    "razao_social": "Razão Social",
    "cnpj": "CNPJ",
    "segmento": "Segmento",
    "cnae_principal": "CNAE Principal",
    "descricao_cnae": "Descrição CNAE",
    "origem_cnae": "Origem do CNAE",
    "porte_empresa": "Porte",
    "matriz_filial": "Matriz/Filial",
    "data_abertura": "Data de Abertura",
    "municipio": "Município",
    "bairro": "Bairro",
    "logradouro": "Logradouro",
    "numero": "Número",
    "complemento": "Complemento",
    "cep": "CEP",
    "endereco_completo": "Endereço Completo",
    "telefone_1": "Telefone 1",
    "telefone_2": "Telefone 2",
    "email": "E-mail",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "precisao_geo": "Precisão Geográfica",
}
COLUNAS_TEXTO = {"CNPJ", "CEP", "Número", "Telefone 1", "Telefone 2", "CNAE Principal"}
LARGURA_MAX = 60


def _largura(serie: pd.Series, cabecalho: str) -> int:
    amostra = serie.astype(str).head(5000)
    maior = int(amostra.str.len().max()) if len(amostra) else 0
    return min(max(maior, len(cabecalho)) + 2, LARGURA_MAX)


def gerar_excel(df: pd.DataFrame, descricao_filtros: str = "") -> bytes:
    """Gera o .xlsx em memória: aba de dados + 'Resumo por Município'."""
    dados = df[[c for c in COLUNAS_EXPORT if c in df.columns]].rename(columns=COLUNAS_EXPORT).copy()
    for c in dados.columns:
        if isinstance(dados[c].dtype, pd.CategoricalDtype):
            dados[c] = dados[c].astype(str)
    if "Data de Abertura" in dados:
        dados["Data de Abertura"] = pd.to_datetime(dados["Data de Abertura"]).dt.date

    resumo = (
        df.assign(segmento=df["segmento"].astype(str), municipio=df["municipio"].astype(str))
        .pivot_table(index="municipio", columns="segmento", values="cnpj",
                     aggfunc="count", fill_value=0, observed=True)
    )
    ordem = ["Hipermercado", "Supermercado", "Atacadista / Atacarejo", "Minimercado / Mercearia"]
    resumo = resumo[[c for c in ordem if c in resumo.columns]
                    + [c for c in resumo.columns if c not in ordem]]
    resumo["Total"] = resumo.sum(axis=1)
    resumo = resumo.sort_values("Total", ascending=False)
    contato = df.assign(
        municipio=df["municipio"].astype(str),
        _tel=(df["telefone_1"].fillna("") != "") | (df["telefone_2"].fillna("") != ""),
        _mail=df["email"].fillna("") != "",
    ).groupby("municipio").agg(com_telefone=("_tel", "sum"), com_email=("_mail", "sum"))
    resumo = resumo.join(contato).rename(columns={"com_telefone": "Com Telefone", "com_email": "Com E-mail"})
    resumo.index.name = "Município"
    resumo = resumo.reset_index()
    if len(resumo):
        total = resumo.drop(columns="Município").sum(numeric_only=True)
        total["Município"] = "TOTAL GERAL"
        resumo = pd.concat([resumo, total.to_frame().T], ignore_index=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter",
                        engine_kwargs={"options": {"strings_to_numbers": False,
                                                   "strings_to_urls": False}}) as xw:
        wb = xw.book
        wb.set_properties({"title": "VarejoRadar-MS — Estabelecimentos",
                           "subject": "Supermercados e atacadistas de MS",
                           "comments": "Fonte: Receita Federal do Brasil — Dados Abertos do CNPJ"})
        f_head = wb.add_format({"bold": True, "font_color": "#FFFFFF", "bg_color": AZUL_MS,
                                "border": 1, "border_color": "#D0D7E2", "valign": "vcenter",
                                "text_wrap": True})
        f_txt = wb.add_format({"num_format": "@"})
        f_data = wb.add_format({"num_format": "dd/mm/yyyy"})
        f_coord = wb.add_format({"num_format": "0.000000"})
        f_tit = wb.add_format({"bold": True, "font_size": 14, "font_color": AZUL_MS})
        f_sub = wb.add_format({"italic": True, "font_color": "#5B6B7F"})
        f_total = wb.add_format({"bold": True, "top": 2, "top_color": OURO_MS, "bg_color": "#FFF7E0"})
        f_int = wb.add_format({"num_format": "#,##0"})

        # --- Aba 1: dados ------------------------------------------------------
        aba = "Estabelecimentos"
        dados.to_excel(xw, sheet_name=aba, index=False, startrow=0)
        ws = xw.sheets[aba]
        ws.set_row(0, 30)
        for j, col in enumerate(dados.columns):
            ws.write(0, j, col, f_head)
            fmt = f_txt if col in COLUNAS_TEXTO else f_data if col == "Data de Abertura" \
                else f_coord if col in ("Latitude", "Longitude") else None
            ws.set_column(j, j, _largura(dados[col], col), fmt)
        # Reescreve colunas-texto como string explícita (CNPJ/CEP nunca viram número)
        for col in COLUNAS_TEXTO & set(dados.columns):
            j = dados.columns.get_loc(col)
            for i, v in enumerate(dados[col].astype(str), start=1):
                ws.write_string(i, j, "" if v in ("nan", "None") else v, f_txt)
        if len(dados):
            ws.autofilter(0, 0, len(dados), len(dados.columns) - 1)
        ws.freeze_panes(1, 1)

        # --- Aba 2: resumo -----------------------------------------------------
        aba2 = "Resumo por Município"
        resumo.to_excel(xw, sheet_name=aba2, index=False, startrow=3)
        ws2 = xw.sheets[aba2]
        ws2.write(0, 0, "Resumo por Município e Segmento", f_tit)
        ws2.write(1, 0, f"Gerado em {datetime.now():%d/%m/%Y %H:%M} · {len(df):,} estabelecimentos"
                  .replace(",", ".") + (f" · Filtros: {descricao_filtros}" if descricao_filtros else ""),
                  f_sub)
        ws2.set_row(3, 30)
        for j, col in enumerate(resumo.columns):
            ws2.write(3, j, col, f_head)
            ws2.set_column(j, j, _largura(resumo[col], col) if j == 0 else max(len(col) + 4, 12),
                           None if j == 0 else f_int)
        if len(resumo):
            ult = 3 + len(resumo)
            for j, v in enumerate(resumo.iloc[-1]):
                ws2.write(ult, j, v, f_total)
        ws2.freeze_panes(4, 1)
    return buf.getvalue()
