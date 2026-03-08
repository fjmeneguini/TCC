# INSTITUTO FEDERAL DE EDUCAÇÃO, CIÊNCIA E TECNOLOGIA DE SÃO PAULO
## Câmpus Campinas

### Curso Superior de Tecnologia em Análise e Desenvolvimento de Sistemas

---

## Arquitetura de Banco de Dados e Pipeline Reprodutível de Engenharia de Dados para Disponibilização Científica das Bases Públicas do SINASC (DATASUS)

**Autor:** Francisco José da Silva Meneguini  
**Orientador:** Prof. Carlos Eduardo Beluzo  
**Local:** Campinas - SP  
**Ano:** 2026

---

## Sobre este TCC

Este projeto tem como foco **engenharia de dados aplicada à saúde pública**, construindo uma infraestrutura reprodutível para transformar os dados brutos do SINASC (DATASUS) em uma base científica pronta para pesquisa.

O objetivo central **não é desenvolver modelos de Machine Learning**, e sim entregar a base estrutural que permite pesquisas confiáveis, rastreáveis e comparáveis ao longo do tempo.

**Recorte temporal vigente do projeto:** dados do SINASC de **2013 em diante**.

Em resumo, o trabalho propõe:

- Download automatizado dos dados públicos do SINASC
- Pipeline de ETL com regras explícitas de limpeza e padronização
- Banco de dados relacional otimizado para consultas científicas
- Views analíticas prontas para uso recorrente
- Documentação técnica para reprodutibilidade e transparência

---

## Problema de pesquisa

Hoje, cada pesquisador que utiliza SINASC normalmente precisa:

- Baixar arquivos brutos manualmente
- Limpar e padronizar os dados do zero
- Resolver diferenças de layout entre anos
- Tratar nulos, códigos inválidos e inconsistências
- Repetir etapas sem padronização entre estudos

Esse processo aumenta retrabalho, risco de erro e reduz reprodutibilidade científica.

---

## Hipótese

A implementação de um pipeline reprodutível de engenharia de dados, integrado a um banco relacional estruturado para o SINASC, reduz o esforço técnico de preparação e aumenta padronização, rastreabilidade e reprodutibilidade das análises.

---

## Objetivo geral

Desenvolver e documentar uma arquitetura de banco de dados relacional e um pipeline reprodutível de engenharia de dados para o SINASC/DATASUS, com ingestão automatizada, padronização e modelagem orientada a consultas científicas.

## Objetivos específicos

1. Automatizar a obtenção de dados oficiais do SINASC.
2. Implementar ETL para leitura, limpeza, padronização e harmonização das variáveis.
3. Modelar banco relacional com schema, chaves, índices e desempenho analítico.
4. Criar views científicas para consultas epidemiológicas e demográficas recorrentes.
5. Realizar EDA estruturada da base carregada.
6. Documentar toda a arquitetura para garantir transparência metodológica.

---

## Escopo técnico do projeto

### 1) Ingestão automatizada
- Download direto de fonte pública (dados.gov.br / DATASUS)
- Organização de arquivos por ano e UF
- Registro de metadados e logs

### 2) Pipeline ETL
- Leitura de arquivos brutos
- Limpeza e padronização
- Conversão de tipos
- Tratamento de ausentes e inconsistências
- Harmonização de colunas entre anos

### 3) Banco de dados (servidor IFSP)
- Definição de schema relacional
- Tabelas normalizadas
- Particionamento por ano
- Índices (com foco em UF e município)
- Tabela de configuração/categorização de variáveis

### 4) Camada analítica
- Views científicas prontas para pesquisa
- Views para baixo peso ao nascer
- Views para prematuridade
- Consultas exemplo para estatística, ML e pesquisa

### 5) Qualidade e desempenho
- Testes de performance de consultas
- Ajustes de índices e estratégias de acesso
- Avaliação de completude e consistência dos dados

---

## Exemplo de uso esperado

```sql
SELECT *
FROM sinasc_view_pronta
WHERE uf = 'SP'
	AND ano BETWEEN 2018 AND 2022
	AND consultas_pre_natal > 6;
```

Sem necessidade de baixar e limpar CSV manualmente para cada novo estudo.

---

## Fases do trabalho e entregas

| Fase | Período | Entrega principal |
|---|---|---|
| Fase 1 - Entendimento do SINASC e variáveis | 24/02 a 03/03 | Documento de mapeamento do SINASC |
| Fase 2 - Download automático dos dados | 03/03 a 10/03 | Script de download + documentação |
| Fase 3 - ETL | 10/03 a 24/03 | Pipeline ETL funcionando |
| Fase 4 - Modelagem do banco | 24/03 a 07/04 | Banco estruturado no servidor IFSP |
| Fase 5 - Views científicas e performance | 07/04 a 21/04 | Conjunto de views documentadas |
| Fase 6 - EDA estruturada | 21/04 a 05/05 | Notebook/relatório de EDA |
| Fase 7 - Demonstração prática de uso | 05/05 a 12/05 | Exemplos práticos de consumo |
| Fase 8 - Escrita do artigo | 12/05 ao final | Redação final e documentação |

---

## Relação com a pesquisa-base

Este TCC se inspira em estudos que utilizam o SINASC em grande escala, inclusive cenários com dezenas de milhões de registros e uso de ambientes analíticos como DuckDB e Python.

A contribuição deste projeto é construir a **infraestrutura de dados** que viabiliza análises robustas:

- Base harmonizada
- Banco otimizado
- Pipeline reproduzível
- Governança de qualidade

Ou seja, o ML é consumidor da estrutura; a entrega deste TCC é a estrutura.

---

## Resumo em uma frase

Transformar dados públicos brutos do SINASC/DATASUS em uma base científica pronta para pesquisa, análise estatística e aplicações futuras de ML.
