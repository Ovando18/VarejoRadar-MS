# 🛒 VarejoRadar-MS

Painel executivo de **inteligência comercial** e **mapeamento georreferenciado** de
supermercados, hipermercados, minimercados e atacadistas dos **79 municípios de
Mato Grosso do Sul**, construído com Streamlit + Folium sobre a base oficial de
CNPJs ativos da Receita Federal.

> **Base atual (RFB, referência 10/2026):** 13.684 estabelecimentos ativos nos
> 79 municípios — 115 hipermercados, 1.291 supermercados, 1.524 atacadistas /
> atacarejos e 10.754 minimercados / mercearias (6.861 pelo CNAE principal e
> 6.823 apenas pelo secundário). 96% com telefone e 84% com e-mail.
> Geolocalização: **78% por logradouro**, 10% por bairro e 12% no centroide do
> município.
>
> 🌐 **Demo:** https://varejoradar-ms.streamlit.app/

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
| Geocodificação | [Nominatim / OpenStreetMap](https://nominatim.org/) respeitando a política de uso (1 req/s), com cache incremental em `data/geocache_nominatim.json`. Hierarquia: **logradouro** (≤ 60 km da sede) → **bairro** (≤ 40 km) → **centroide do município**, sempre com jitter determinístico pelo hash do CNPJ (120 m / 350 m / 1,5 km) para os pontos não se sobreporem. A coluna `precisao_geo` indica o nível. Todos os registros recebem coordenada dentro de MS. |
| Saída | `data/estabelecimentos_ms.parquet` (zstd) + `data/metadata.json` |

> **Por que não geocodificar por CEP?** A BrasilAPI CEP v2 foi testada: para ~99% dos CEPs de MS ela devolve a mesma coordenada genérica (centro de Campo Grande), o que empilharia os pontos. Por isso o pipeline usa logradouro/bairro via OSM.

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
│   └── geocache_nominatim.json  # cache logradouro/bairro → coordenadas
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

A RFB publica uma nova extração por mês. O ETL baixa ~6,4 GB em paralelo
(~7 min a 16 MB/s; brutos apagados ao final), filtra em ~7 min e reaproveita o
cache de geocodificação — só endereços novos são consultados. A primeira
geocodificação completa (~9,3 mil bairros/logradouros a 1 req/s) leva ~2,5 h;
use `--geo-budget-min` para limitar e completar em execuções seguintes:

```bash
python scripts/fetch_data.py
```

Opções: `--mes AAAA-MM`, `--keep-raw`, `--skip-download` (reprocessa brutos já
baixados, usando o resultado filtrado intermediário se existir),
`--no-geocode` (usa só o cache), `--geo-budget-min N`. Variável:
`VR_DOWNLOAD_WORKERS` (conexões paralelas, padrão 16).

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
- Pontos com `precisao_geo = "Centroide do município"` ou `"Bairro"` têm posição
  aproximada; o link do Google Maps nesses casos busca pelo endereço textual.
- A classificação segue o CNAE declarado à RFB. Ex.: as lojas Assaí estão
  cadastradas como 4711-3/01 (hipermercado), não como atacadista.
- Geocodificação © colaboradores do OpenStreetMap (ODbL). Tiles do mapa © Esri.
