# Data Quality

## Objetivo

O módulo de Data Quality existe para validar automaticamente as camadas Silver e Gold antes do consumo analítico. O foco é demonstrar que um pipeline Lakehouse não termina na transformação: ele também precisa produzir confiança operacional e semântica.

As checagens estão implementadas em [src/quality/data_quality_checks.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/src/quality/data_quality_checks.py).

## Papel da Data Quality na arquitetura

No laboratório, a Data Quality funciona como um controle entre modelagem e consumo:

- identifica inconsistências antes do uso analítico;
- documenta falhas encontradas;
- gera evidências em Markdown e JSON;
- pode ser executada isoladamente ou dentro do pipeline orquestrado.

## Artefatos gerados

| Artefato | Objetivo |
| --- | --- |
| `reports/data_quality/data_quality_report.md` | leitura humana e evidência de execução |
| `reports/data_quality/data_quality_results.json` | consumo por scripts, automação e integração |

## Regras da camada Silver

| Entidade | Regra | Objetivo |
| --- | --- | --- |
| `customers` | `customer_id` não pode ser nulo | garantir chave mínima da entidade |
| `customers` | e-mails inválidos devem ser sinalizados | preservar o registro, mas explicitar problema |
| `products` | `product_id` não pode ser nulo | evitar produto órfão no consumo analítico |
| `orders` | `order_id` não pode ser nulo | preservar rastreabilidade do pedido |
| `orders` | `order_status` deve estar na lista permitida | impedir status fora do contrato analítico |
| `order_items` | `quantity` deve ser maior que zero para válidos | validar coerência de item vendável |
| `order_items` | `net_amount` não deve ser negativo para válidos | proteger métricas de receita |

## Regras da camada Gold

| Entidade | Regra | Objetivo |
| --- | --- | --- |
| `dim_customer` | `customer_key` não pode ser nulo | garantir integridade dimensional |
| `dim_product` | `product_key` não pode ser nulo | garantir integridade dimensional |
| `dim_campaign` | `campaign_key` não pode ser nulo | garantir integridade dimensional |
| `dim_date` | `date_key` não pode ser nulo | garantir navegabilidade temporal |
| `dim_date` | `date_key` deve ser único | evitar ambiguidade temporal |
| `fct_sales` | `customer_key` e `product_key` devem existir | manter legibilidade analítica mínima |
| `fct_sales` | `net_amount` deve ser numérico | proteger agregações de receita |
| `fct_sales` | `order_item_id` não pode duplicar | proteger grão da fato de vendas |

## Estratégia adotada

Uma decisão importante do projeto é não descartar automaticamente todo registro problemático. Em alguns casos, o dado continua existindo na Silver com sinalização explícita. Isso é útil para mostrar uma abordagem realista:

- nem todo erro significa apagar a linha;
- parte da governança está em sinalizar e medir;
- a qualidade pode ser progressiva entre camadas.

## Como executar

```bash
python3 src/quality/data_quality_checks.py \
  --silver-dir data/silver \
  --gold-dir data/gold \
  --report-path reports/data_quality/data_quality_report.md \
  --json-path reports/data_quality/data_quality_results.json \
  --master local[*]
```

## Como os resultados devem ser lidos

Cada regra retorna:

- camada avaliada;
- entidade;
- nome da regra;
- descrição;
- total de registros avaliados;
- total de falhas;
- status booleano em `passed`;
- percentual aproximado de qualidade.

Isso permite diferenciar:

- datasets corretos;
- datasets com warnings;
- regressões técnicas entre execuções.

## Relação com testes automatizados

Além da execução como script, o projeto também possui testes em `tests/data_quality/`. Isso ajuda a validar:

- a lógica das regras;
- compatibilidade com a estrutura atual da Silver e da Gold;
- regressões ao evoluir o pipeline.

## Limites da abordagem

Este módulo não pretende substituir:

- um catálogo corporativo de regras;
- observabilidade enterprise com alertas em tempo real;
- frameworks completos de data contracts;
- políticas regulatórias formais.

O objetivo é demonstrar Data Quality aplicada, reproduzível e útil em um laboratório técnico local.
