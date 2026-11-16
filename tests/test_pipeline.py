"""Testes rápidos: padronização do ETL, filtros e exportação Excel.

Execute com:  python -m pytest -q
"""
import io
import sys
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import fetch_data as fd  # noqa: E402
from utils.data_loader import Filtros, aplicar_filtros, normalizar  # noqa: E402
from utils.excel_export import gerar_excel  # noqa: E402


def test_formatadores():
    assert fd.fmt_cnpj("12345678", "0001", "95") == "12.345.678/0001-95"
    assert fd.fmt_cep("79002000") == "79002-000"
    assert fd.fmt_cep("123") == ""
    assert fd.fmt_tel("67", "33211234") == "(67) 3321-1234"
    assert fd.fmt_tel("67", "999887766") == "(67) 99988-7766"
    assert fd.fmt_tel("", "00000000") == ""
    assert fd.fmt_email(" Contato@Mercado.COM.BR ") == "contato@mercado.com.br"
    assert fd.fmt_email("invalido@") == ""
    assert fd.fmt_cnae("4711302") == "4711-3/02"
    assert fd.titulo("RUA DOM AQUINO DA SILVA") == "Rua Dom Aquino da Silva"
    assert fd.titulo("VILA ROMA II") == "Vila Roma II"
    assert fd.titulo("BR 163") == "BR 163"


def test_jitter_deterministico_e_limitado():
    a = fd.jitter_deterministico("12.345.678/0001-95", 2.0)
    assert a == fd.jitter_deterministico("12.345.678/0001-95", 2.0)
    assert (a[0] ** 2 + a[1] ** 2) ** 0.5 * 111 <= 2.0 + 1e-9


def test_classificacao_principal_tem_precedencia():
    class R:
        cnae_principal = "4712100"
        cnae_secundaria = "4711302"
    assert fd.classificar(R) == ("Minimercado / Mercearia", "4712100", "Principal")

    class S:
        cnae_principal = "1091102"
        cnae_secundaria = "4712100,4711302"
    assert fd.classificar(S) == ("Supermercado", "4711302", "Secundário")


@pytest.fixture
def amostra():
    df = pd.DataFrame({
        "cnpj": ["11.111.111/0001-11", "22.222.222/0001-22", "33.333.333/0001-33"],
        "nome_fantasia": ["SUPER CENTRAL", "ATACADÃO SUL", "MERCEARIA SÃO JOSÉ"],
        "razao_social": ["CENTRAL LTDA", "SUL ATACADO SA", "JOSE DA SILVA ME"],
        "segmento": ["Supermercado", "Atacadista / Atacarejo", "Minimercado / Mercearia"],
        "origem_cnae": ["Principal", "Principal", "Secundário"],
        "cnae_principal": ["4711-3/02", "4691-5/00", "1091-1/02"],
        "descricao_cnae": ["x", "y", "z"],
        "porte_empresa": ["EPP", "Demais", "ME"],
        "matriz_filial": ["Matriz"] * 3,
        "data_abertura": pd.to_datetime(["2010-01-01", "2015-05-05", None]),
        "municipio": ["Campo Grande", "Dourados", "Campo Grande"],
        "bairro": ["Centro", "Jardim América", "Coronel Antonino"],
        "logradouro": ["Rua A", "Av B", "Rua C"], "numero": ["1", "2", "S/N"],
        "complemento": ["", "", ""], "cep": ["79002-000", "79800-000", "79010-000"],
        "endereco_completo": ["e1", "e2", "e3"],
        "telefone_1": ["(67) 3321-1234", "", "(67) 99988-7766"],
        "telefone_2": ["", "", ""], "email": ["a@b.com", "", ""],
        "latitude": [-20.4, -22.2, -20.45], "longitude": [-54.6, -54.8, -54.62],
        "precisao_geo": ["CEP", "Centroide do município", "CEP"],
    })
    df["tem_telefone"] = df["telefone_1"] != ""
    df["tem_email"] = df["email"] != ""
    df["_busca"] = (df["nome_fantasia"] + " " + df["razao_social"] + " " + df["bairro"] + " "
                    + df["cnpj"].str.replace(r"\D", "", regex=True)).map(normalizar)
    return df


def test_filtros(amostra):
    assert len(aplicar_filtros(amostra, Filtros())) == 2  # secundário excluído por padrão
    assert len(aplicar_filtros(amostra, Filtros(incluir_secundario=True))) == 3
    assert len(aplicar_filtros(amostra, Filtros(municipios=["Dourados"]))) == 1
    assert len(aplicar_filtros(amostra, Filtros(so_telefone=True))) == 1
    assert len(aplicar_filtros(amostra, Filtros(so_email=True))) == 1
    assert len(aplicar_filtros(amostra, Filtros(busca="atacadao"))) == 1  # sem acento
    assert len(aplicar_filtros(amostra, Filtros(busca="22.222.222/0001"))) == 1
    assert len(aplicar_filtros(amostra, Filtros(busca="sao jose", incluir_secundario=True))) == 1


def test_excel(amostra):
    conteudo = gerar_excel(amostra, "teste")
    wb = load_workbook(io.BytesIO(conteudo))
    assert wb.sheetnames == ["Estabelecimentos", "Resumo por Município"]
    ws = wb["Estabelecimentos"]
    cab = [c.value for c in ws[1]]
    assert cab[:3] == ["Nome Fantasia", "Razão Social", "CNPJ"]
    assert ws.cell(1, 1).fill.fgColor.rgb.endswith("004B87")
    col_cnpj = cab.index("CNPJ") + 1
    assert ws.cell(2, col_cnpj).value == "11.111.111/0001-11"
    assert ws.cell(2, col_cnpj).data_type == "s"
    resumo = wb["Resumo por Município"]
    assert resumo.cell(4, 1).value == "Município"
    valores = [r[0] for r in resumo.iter_rows(min_row=5, values_only=True)]
    assert "TOTAL GERAL" in valores
