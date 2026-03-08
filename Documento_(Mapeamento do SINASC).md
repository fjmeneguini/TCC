# Mapeamento da Base SINASC — Recorte 2013 a 2023

## 1) Escopo e objetivo

Este documento consolida o mapeamento técnico da base SINASC no recorte **2013–2023**, cobrindo variáveis, tipos esperados e problemas de qualidade de dados para suportar a etapa ETL e a modelagem relacional.

## 2) Fontes de referência

- Portal de dados públicos (dados.gov.br / OpenDataSUS) para os arquivos anuais.
- Dicionário/definições do SINASC (DATASUS) como referência semântica dos campos.
- Evidência estrutural do recorte no projeto:
	- `data/raw/2013` até `data/raw/2023`
	- `data/processed/sinasc_harmonized_columns.json` (colunas harmonizadas 2013–2023)

## 3) Inventário de variáveis relevantes (2013–2023)

As colunas abaixo foram identificadas na harmonização do recorte 2013–2023.

| Coluna | Tipo esperado | Natureza |
|---|---|---|
| CONTADOR | inteiro | identificador técnico |
| ORIGEM | categórico (código) | origem da informação |
| CODCART | categórico (código) | cartório |
| CODMUNCART | inteiro/código IBGE | município do cartório |
| NUMREGCART | texto | número de registro cartorial |
| DTREGCART | data (DDMMAAAA) | data de registro em cartório |
| CODESTAB | inteiro/código CNES | estabelecimento de saúde |
| CODMUNNASC | inteiro/código IBGE | município de nascimento |
| LOCNASC | categórico (código) | local de nascimento |
| IDADEMAE | inteiro | idade materna |
| ESTCIVMAE | categórico (código) | estado civil materno |
| ESCMAE | categórico (código) | escolaridade (modelo antigo) |
| CODOCUPMAE | categórico (código) | ocupação materna |
| QTDFILVIVO | inteiro | filhos vivos |
| QTDFILMORT | inteiro | filhos mortos/perdas |
| CODMUNRES | inteiro/código IBGE | município de residência |
| CODPAISRES | categórico (código) | país de residência |
| GRAVIDEZ | categórico (código) | tipo de gravidez |
| PARTO | categórico (código) | tipo de parto |
| CONSULTAS | categórico (código) | faixa de consultas pré-natal |
| DTNASC | data (DDMMAAAA) | data de nascimento |
| HORANASC | texto/hora | horário de nascimento |
| SEXO | categórico (código) | sexo do recém-nascido |
| APGAR1 | inteiro | Apgar 1º minuto |
| APGAR5 | inteiro | Apgar 5º minuto |
| RACACORN | categórico (código) | raça/cor do RN |
| RACACORMAE | categórico (código) | raça/cor da mãe |
| RACACOR | categórico (código) | raça/cor do RN (variação histórica) |
| PESO | inteiro | peso ao nascer (gramas) |
| IDANOMAL | categórico (código) | indicador de anomalia |
| CODANOMAL | texto/código | código da anomalia |
| DTCADASTRO | data (DDMMAAAA) | data de cadastro |
| NUMEROLOTE | inteiro/texto | lote de processamento |
| VERSAOSIST | texto | versão do sistema |
| DTRECEBIM | data (DDMMAAAA) | data de recebimento |
| DTRECORIG | data (DDMMAAAA) | data de recebimento origem |
| DIFDATA | inteiro | diferença entre datas |
| NATURALMAE | categórico (código) | naturalidade materna |
| CODMUNNATU | inteiro/código IBGE | município de naturalidade |
| CODUFNATU | categórico (UF) | UF de naturalidade |
| DTNASCMAE | data (DDMMAAAA) | data de nascimento da mãe |
| QTDGESTANT | inteiro | gestações anteriores |
| QTDPARTNOR | inteiro | partos vaginais anteriores |
| QTDPARTCES | inteiro | partos cesáreos anteriores |
| IDADEPAI | inteiro | idade do pai |
| DTULTMENST | data (DDMMAAAA) | data da última menstruação |
| MESPRENAT | inteiro | mês de início do pré-natal |
| TPAPRESENT | categórico (código) | tipo de apresentação fetal |
| STTRABPART | categórico (código) | trabalho de parto |
| STCESPARTO | categórico (código) | cesárea antes do trabalho de parto |
| TPNASCASSI | categórico (código) | tipo de assistência ao nascimento |
| STDNEPIDEM | categórico (código) | status de DN |
| STDNNOVA | categórico (código) | status de DN nova |
| SERIESCMAE | categórico (código) | série escolar materna |
| ESCMAEAGR1 | categórico (código) | escolaridade agrupada |
| SEMAGESTAC | inteiro | semanas de gestação |
| GESTACAO | categórico (código) | faixa de gestação |
| TPMETESTIM | categórico (código) | método de estimação |
| ESCMAE2010 | categórico (código) | escolaridade (modelo 2010+) |
| DTRECORIGA | data (DDMMAAAA) | data de recebimento origem (variação de nome) |
| CONSPRENAT | inteiro | número de consultas pré-natal |
| TPFUNCRESP | categórico (código) | função responsável pela declaração |
| TPDOCRESP | categórico (código) | tipo de documento do responsável |
| DTDECLARAC | data (DDMMAAAA) | data de declaração |
| TPROBSON | categórico (código) | classificação de Robson |
| PARIDADE | categórico/inteiro | paridade |
| KOTELCHUCK | categórico (código) | índice de adequação pré-natal |
| OPORT_DN | categórico (código) | oportunidade da DN |

## 4) Mapeamento de problemas comuns de qualidade

Problemas observados e esperados no recorte 2013+:

- Valores ausentes representados por vazio, `NA`, `N/A`, `NULL`, `IGN`, `IGNORADO`.
- Códigos de ignorado em variáveis categóricas (ex.: `9`, e em alguns casos `0`).
- Datas em formato numérico textual `DDMMAAAA` (exigem validação e conversão).
- Códigos com tamanhos diferentes (municípios e estabelecimento de saúde).
- Variação histórica de nomes/estrutura de colunas entre anos.

## 5) Regras técnicas esperadas para ETL (derivadas do mapeamento)

- Padronizar nomes de coluna em uppercase.
- Harmonizar colunas por união dos cabeçalhos do recorte 2013–2023.
- Normalizar município para 6 dígitos (`CODMUN*`) e enriquecer com referência IBGE.
- Normalizar `CODESTAB` para 7 dígitos e validar consistência.
- Converter datas para formato ISO (`AAAA-MM-DD`) quando válidas.
- Converter métricas numéricas (idade, peso, apgar, consultas) para inteiro válido.
- Manter tabela de categorias para variáveis codificadas (sexo, raça/cor, parto, gestação).