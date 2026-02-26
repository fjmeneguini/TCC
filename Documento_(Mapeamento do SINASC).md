# Mapeamento da Base SINASC – Ano 2023

## 1. Origem dos Dados

Fonte: Portal Brasileiro de Dados Abertos (dados.gov.br)  
Conjunto de dados: Sistema de Informação sobre Nascidos Vivos – SINASC  
Órgão responsável: Ministério da Saúde (DATASUS)  
Arquivo analisado: Nascidos Vivos – 2023 (formato CSV)

O arquivo foi importado utilizando Power Query devido ao grande volume de registros, evitando limitações do Excel em relação ao número máximo de linhas.

---

## 2. Estrutura Geral da Base

A base do SINASC é composta por variáveis relacionadas a:

- Identificação do registro
- Informações maternas
- Dados da gestação
- Dados do parto
- Informações do recém-nascido
- Informações administrativas e de sistema

O conjunto analisado apresenta aproximadamente dezenas de colunas, majoritariamente armazenadas como valores numéricos representando códigos categóricos.

---

## 3. Principais Variáveis Identificadas

### 3.1 Identificação do Registro

| Coluna       | Descrição                                      |Tipo Esperado |
|--------------|------------------------------------------------|--------------|
| CONTADOR     | Identificador do registro                      | Inteiro      |
| NUMEROLOTE   | Número do lote                                 | Inteiro      |
| ORIGEM       | Banco de dados de origem                       | Código       |
| CODESTAB     | Código do estabelecimento (CNES)               | Inteiro      |
| CODMUNNASC   | Código IBGE do município de nascimento         | Inteiro      |
| CODMUNRES    | Código IBGE do município de residência         | Inteiro      |

---

### 3.2 Informações da Mãe

| Coluna       | Descrição                                     |  Tipo   |
|--------------|-----------------------------------------------|---------|
| IDADEMAE     | Idade da mãe                                  | Inteiro |
| ESTCIVMAE    | Situação conjugal da mãe                      | Código  |
| ESCMAE       | Escolaridade (modelo antigo)                  | Código  |
| ESCMAE2010   | Escolaridade (modelo atualizado)              | Código  |
| RACACORMAE   | Raça/cor da mãe                               | Código  |
| QTDFILVIVO   | Número de filhos vivos                        | Inteiro |
| QTDFILMORT   | Número de perdas fetais e abortos             | Inteiro |
| QTDGESTANT   | Número de gestações anteriores                | Inteiro |
| QTDPARTNOR   | Número de partos vaginais                     | Inteiro |
| QTDPARTCES   | Número de partos cesáreos                     | Inteiro |
| IDADEPAI     | Idade do pai                                  | Inteiro |

---

### 3.3 Dados da Gestação e Parto

| Coluna       | Descrição                                     |  Tipo  |
|--------------|-----------------------------------------------|--------|
| GESTACAO     | Faixa de semanas de gestação                  | Código |
| SEMAGESTAC   | Número de semanas de gestação                 | Inteiro|
| GRAVIDEZ     | Tipo de gravidez                              | Código |
| PARTO        | Tipo de parto                                 | Código |
| TPAPRESENT   | Tipo de apresentação do recém-nascido         | Código |
| STTRABPART   | Trabalho de parto induzido                    | Código |
| STCESPARTO   | Cesárea antes do trabalho de parto            | Código |

---

### 3.4 Dados do Recém-Nascido

| Coluna       | Descrição                                     |   Tipo  |
|--------------|-----------------------------------------------|---------|
| SEXO         | Sexo do recém-nascido                         |  Código |
| PESO         | Peso ao nascer (gramas)                       | Inteiro |
| APGAR1       | Índice Apgar no 1º minuto                     | Inteiro |
| APGAR5       | Índice Apgar no 5º minuto                     | Inteiro |
| RACACOR      | Raça/cor do recém-nascido                     |  Código |
| IDANOMAL     | Presença de anomalia                          |  Código |
| CODANOMAL    | Código da anomalia (CID-10)                   |  Texto  |

---

### 3.5 Informações de Pré-Natal

| Coluna       | Descrição                                     |   Tipo  |
|--------------|-----------------------------------------------|---------|
| CONSULTAS    | Categoria de número de consultas              |  Código |
| CONSPRENAT   | Número real de consultas                      | Inteiro |
| MESPRENAT    | Mês de início do pré-natal                    | Inteiro |
| KOTELCHUCK   | Índice de adequação da assistência pré-natal  |  Código |

---

## 4. Tipos de Dados Observados

A maioria das variáveis é armazenada como número inteiro, mesmo quando representa categorias.  
Alguns campos utilizam códigos padronizados (ex: 1, 2, 3, 9), onde:

- 9 geralmente representa “Ignorado”
- 0 pode representar ausência ou categoria específica dependendo do campo

Campos de data (DTNASC, DTCADASTRO, DTDECLARAC, etc.) estão armazenados como valores numéricos no formato DDMMYYYY, exigindo conversão no processo de ETL.

---

## 5. Problemas Identificados

Durante a análise inicial foi observado:

- Uso de códigos numéricos para representar categorias
- Presença de códigos “9” ou “0” indicando valores ignorados
- Datas armazenadas como inteiros
- Possível inconsistência de tipos detectados automaticamente pelo Excel/Power Query
- Mudanças estruturais ao longo dos anos conforme descrito no dicionário

---

## 6. Considerações para a Próxima Fase (ETL)

Na etapa de ETL será necessário:

- Converter datas para formato padrão
- Tratar códigos de valores ignorados
- Padronizar variáveis categóricas
- Garantir consistência entre anos diferentes
- Definir tipos corretos para modelagem no banco de dados