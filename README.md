# 🛒 VarejoRadar-MS

Painel executivo de **inteligência comercial** e **mapeamento georreferenciado** de
supermercados, hipermercados, minimercados e atacadistas dos **79 municípios de
Mato Grosso do Sul**, construído com Streamlit + Folium sobre a base oficial de
CNPJs ativos da Receita Federal.

<!-- STATS -->

## Funcionalidades

- **Filtros inteligentes** (barra lateral): município(s) com atalho para polos
  regionais, segmento, porte (ME / EPP / Demais), apenas com telefone e/ou
  e-mail, busca livre (nome fantasia, razão social, CNPJ com ou sem máscara,
  bairro — sem diferenciar acentos) e opção avançada para incluir
  estabelecimentos cujo CNAE-alvo é apenas **secundário**.
- **KPIs dinâmicos**: total filtrado, supermercados & hipermercados,
  atacadistas & atacarejos, municípios abrangidos e % com telefone/e-mail.
- **Mapa interativo** (Folium + `MarkerCluster`) com cores por segmento,
  legenda, popup HTML rico (dados cadastrais + link para o Google Maps) e
  **cartão de detalhes** do ponto clicado (via `last_object_clicked`), com bloco
  de texto pronto para cópia.
- **Tabela interativa** e **exportação `.xlsx`** profissional: cabeçalho Azul MS,
  largura automática, CNPJ/CEP/telefones como texto, autofiltro, painéis
  congelados e aba **"Resumo por Município"** (contagem por cidade × segmento,
  contatos e total geral).
- Gráficos de distribuição (top 15 municípios, porte, contato).

## Fonte de dados e pipeline (`scripts/fetch_data.py`)

| Etapa | Detalhe |
|---|---|
| Fonte | [Dados Abertos do CNPJ — Receita Federal](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/) (Estabelecimentos, Empresas, Cnaes, Municípios), mês mais recente publicado |
| Download | Paralelo por faixas de bytes (HTTP Range) via WebDAV público, com retomada (`*.parts.json`) |
| Filtro | `UF = MS`, situação cadastral **02 – ATIVA**, CNAE principal **ou** secundário em: 4711-3/01, 4711-3/02, 4712-1/00, 4639-7/01, 4639-7/02, 4691-5/00 |
| Enriquecimento | Razão social e porte (arquivos Empresas) |
| Município | Código SIAFI da RFB ↔ IBGE + centroide ([kelvins/municipios-brasileiros](https://github.com/kelvins/municipios-brasileiros)); validação dos 79 municípios |
| Geocodificação | CEP → coordenadas via [BrasilAPI CEP v2](https://brasilapi.com.br/docs#tag/CEP-V2) (cache em `data/geocache_cep.json`). Coordenadas são aceitas só se caírem em MS e a ≤ 120 km do centroide do município; caso contrário, usa-se o **centroide do município + jitter determinístico** (hash do CNPJ, raio 2 km). A coluna `precisao_geo` indica a origem. |
| Saída | `data/estabelecimentos_ms.parquet` (zstd) + `data/metadata.json` |

### Classificação de segmento

| CNAE | Segmento |
|---|---|
| 4711-3/01 | Hipermercado |
| 4711-3/02 | Supermercado |
| 4639-7/01, 4639-7/02, 4691-5/00 | Atacadista / Atacarejo |
| 4712-1/00 | Minimercado / Mercearia |

O CNAE **principal** tem precedência. Quando apenas um CNAE secundário é alvo, o
segmento segue a prioridade Hipermercado → Supermercado → Atacadista →
Minimercado, e o registro recebe `origem_cnae = "Secundário"` (oculto por padrão
no painel).

### Campos do dataset

`cnpj`, `razao_social`, `nome_fantasia`, `segmento`, `origem_cnae`,
`cnae_segmento`, `cnae_principal`, `descricao_cnae`, `porte_empresa`,
`matriz_filial`, `situacao_cadastral`, `data_abertura`, `municipio`,
`codigo_ibge`, `bairro`, `logradouro`, `numero`, `complemento`, `cep`,
`endereco_completo`, `telefone_1`, `telefone_2`, `email`, `latitude`,
`longitude`, `precisao_geo`.

## Estrutura

```text
VarejoRadar-MS/
├── .streamlit/config.toml     # tema (Azul MS #004B87)
├── data/
│   ├── estabelecimentos_ms.parquet
│   ├── metadata.json          # mês de referência, contagens
│   └── geocache_cep.json      # cache CEP → coordenadas
├── scripts/fetch_data.py      # ETL completo
├── utils/
│   ├── data_loader.py         # carga cacheada + filtros
│   └── excel_export.py        # exportação .xlsx
├── tests/test_pipeline.py
├── app.py
├── requirements.txt
└── README.md
```

## Executar localmente

```bash
python -m venv .venv && source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

```bash
streamlit run app.py
```

### Atualizar a base (mensal)

A RFB publica uma nova extração por mês. O ETL baixa ~6,4 GB (arquivos brutos
apagados ao final), leva ~10–20 min e reaproveita o cache de CEPs:

```bash
python scripts/fetch_data.py
```

Opções: `--mes AAAA-MM`, `--keep-raw`, `--skip-download` (reprocessa brutos já
baixados), `--no-geocode`. Variáveis: `VR_DOWNLOAD_WORKERS` (padrão 16),
`VR_GEOCODE_WORKERS` (padrão 6).

### Testes

```bash
pip install pytest openpyxl && python -m pytest -q
```

## Deploy no Streamlit Community Cloud

1. Publique o repositório no GitHub (o parquet em `data/` é versionado; os
   brutos em `data/raw/` são ignorados pelo `.gitignore`).
2. Em [share.streamlit.io](https://share.streamlit.io) → **Create app** →
   selecione o repositório, branch `main` e arquivo `app.py`.
3. Python 3.11+ (o app não chama APIs externas em tempo de execução — tudo vem
   do parquet local, então carrega instantaneamente).

## Avisos

- Dados públicos da RFB: telefones/e-mails são os declarados no cadastro e podem
  estar desatualizados. O uso para prospecção deve observar a **LGPD**
  (Lei 13.709/2018) — em especial para empresários individuais (MEI/EI), cujos
  dados de contato podem ser pessoais.
- Pontos com `precisao_geo = "Centroide do município"` têm posição aproximada.
