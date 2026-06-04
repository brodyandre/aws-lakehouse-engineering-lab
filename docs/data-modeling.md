# Modelagem de dados

## Objetivo da modelagem

A camada Gold deste laboratório foi desenhada para responder perguntas analíticas de negócio com baixo atrito técnico. O modelo privilegia leitura simples, clareza semântica e facilidade de uso por pessoas de dados, BI e analytics engineering.

Por isso, a modelagem adotada foi `Star Schema`.

## Contexto de negócio

O cenário simulado combina:

- clientes;
- produtos;
- campanhas;
- pedidos;
- itens de pedido;
- eventos digitais.

Esse conjunto permite conectar receita, comportamento do cliente e esforço de marketing em um modelo analítico relativamente simples, mas rico o suficiente para exercícios de engenharia de dados.

## O que é Star Schema

Star Schema é um padrão de modelagem dimensional no qual:

- tabelas fato concentram eventos e métricas;
- tabelas dimensão concentram atributos descritivos;
- os relacionamentos entre fatos e dimensões são feitos por chaves substitutas;
- o objetivo principal é facilitar consumo analítico.

O nome vem do formato visual: uma fato central cercada por dimensões.

## Por que Star Schema foi escolhido

| Critério | Motivo da escolha |
| --- | --- |
| Simplicidade de consulta | menos complexidade para receita, clientes, produtos e campanhas |
| Boa comunicação com BI | padrão reconhecido por recrutadores e times analíticos |
| Facilidade de agregação | fatos e dimensões se conectam de forma direta |
| Compatibilidade com o laboratório | combina bem com a Silver conformada e com consultas SQL simples |

## Tabelas da camada Gold

### Dimensões

| Tabela | Papel | Chave |
| --- | --- | --- |
| `dim_customer` | atributos de cliente e localização | `customer_key` |
| `dim_product` | atributos de produto e categoria | `product_key` |
| `dim_campaign` | atributos de campanha, canal e orçamento | `campaign_key` |
| `dim_date` | calendário analítico | `date_key` |

### Fatos

| Tabela | Grão | Métricas principais |
| --- | --- | --- |
| `fct_sales` | um item de pedido válido | `quantity`, `gross_amount`, `discount_amount`, `net_amount` |
| `fct_web_events` | um evento digital | volume de eventos e atributos de navegação |

## Grão das tabelas

Definir o grão corretamente é uma decisão central de modelagem.

| Tabela | Grão definido |
| --- | --- |
| `fct_sales` | uma linha por `order_item_id` válido |
| `fct_web_events` | uma linha por evento web |
| `dim_customer` | um cliente por `customer_id` |
| `dim_product` | um produto por `product_id` |
| `dim_campaign` | uma campanha por `campaign_id` |
| `dim_date` | uma data de calendário por dia |

## Relacionamentos principais

| Origem | Destino |
| --- | --- |
| `fct_sales.customer_key` | `dim_customer.customer_key` |
| `fct_sales.product_key` | `dim_product.product_key` |
| `fct_sales.campaign_key` | `dim_campaign.campaign_key` |
| `fct_sales.date_key` | `dim_date.date_key` |
| `fct_web_events.customer_key` | `dim_customer.customer_key` |
| `fct_web_events.campaign_key` | `dim_campaign.campaign_key` |
| `fct_web_events.date_key` | `dim_date.date_key` |

## Estratégia de chaves

O projeto usa `surrogate keys` determinísticas geradas a partir das chaves naturais da Silver. Essa escolha foi feita para:

- separar identificador técnico de identificador de negócio;
- manter reprodutibilidade em ambiente local;
- simplificar joins nas fatos;
- aproximar o desenho do que se espera em ambientes analíticos reais.

## Perguntas de negócio suportadas

Com o modelo atual, é possível responder, por exemplo:

- qual a receita por categoria;
- qual a receita por mês;
- quais campanhas estão associadas a maior receita;
- quais clientes mais compram;
- quais padrões de navegação antecedem compras.

## Star Schema, Snowflake e Data Vault

| Modelo | Característica | Quando faz mais sentido |
| --- | --- | --- |
| Star Schema | dimensões mais desnormalizadas e consultas simples | BI, dashboards, métricas e consumo analítico |
| Snowflake | dimensões mais normalizadas | cenários com hierarquias detalhadas e menos foco em simplicidade |
| Data Vault | hubs, links e satellites com forte rastreabilidade | integração corporativa complexa e historização extensiva |

## Por que não Snowflake ou Data Vault neste projeto

Snowflake e Data Vault são modelos válidos, mas não eram a melhor escolha para este laboratório.

Motivos:

- o objetivo principal é consumo analítico e demonstração didática;
- o escopo do projeto favorece legibilidade e velocidade de entendimento;
- Star Schema comunica melhor a intenção para recrutadores e times de BI;
- Data Vault adicionaria complexidade além do necessário para este caso de uso.

## Papel de cada camada até a Gold

| Camada | Papel para a modelagem |
| --- | --- |
| `Raw` | preservar a origem |
| `Bronze` | preparar estrutura técnica mínima |
| `Silver` | consolidar entidades conformadas e regras de negócio |
| `Gold` | publicar modelo analítico consumível |

## Limites da modelagem atual

O modelo foi desenhado para ser claro e demonstrativo. Ele não tenta reproduzir:

- SCDs complexos;
- hierarquias profundas;
- satélites históricos;
- planejamento corporativo de MDM;
- catálogo semântico enterprise.

Ainda assim, ele cobre o suficiente para demonstrar organização dimensional, clareza de grão e capacidade analítica realista em um laboratório local.
