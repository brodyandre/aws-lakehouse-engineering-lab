# DE-102 — Possível superestimação de receita na camada Analytics

- Issue: #4
- Branch: `investigation/de-102-analytics-revenue-status`
- Data da investigação: 2026-09-14
- Status: requisito semântico de receita não definido

## Contexto

Durante uma revisão dos indicadores analíticos surgiu a suspeita de que as métricas de receita poderiam estar contabilizando pedidos que não deveriam necessariamente compor receita efetiva.

O pipeline executa com sucesso e os testes automatizados existentes estão aprovados. Por isso, a investigação teve como objetivo determinar se havia uma falha técnica de transformação ou uma divergência relacionada à definição de negócio da métrica de receita.

A análise foi realizada utilizando o banco local:

`data/serving/lakehouse.duckdb`

Nenhum dado, SQL ou código do pipeline foi modificado durante a etapa inicial de diagnóstico.

## Evidências coletadas

### Receita por status do pedido

A tabela `gold.fct_sales` foi agrupada por `order_status` para identificar quantidade de registros, quantidade de pedidos distintos e receita líquida associada.

| order_status | sales_rows | distinct_orders | net_revenue |
| --- | ---: | ---: | ---: |
| cancelled | 222 | 94 | 328252.48 |
| created | 175 | 69 | 279216.30 |
| shipped | 183 | 77 | 275168.43 |
| paid | 178 | 76 | 273154.79 |
| refunded | 179 | 68 | 260335.23 |

Todos os status válidos presentes na `fct_sales` possuem valores positivos associados a `net_amount`.

Isso inclui estados como:

- `cancelled`;
- `refunded`;
- `created`.

A existência desses estados na Gold não representa, isoladamente, um erro, pois todos fazem parte do domínio válido de status de pedido.

Entretanto, um status tecnicamente válido não implica necessariamente que o valor associado deva ser reconhecido como receita.

## Reconciliação Gold versus Analytics

Foi comparado o total publicado em `analytics.revenue_by_month` com a soma de `net_amount` existente na `gold.fct_sales`.

Resultado:

| Métrica | Valor |
| --- | ---: |
| Receita publicada na Analytics | 1416127.23 |
| Receita total da Gold | 1416127.23 |
| Receita somente `paid` + `shipped` | 548323.22 |
| Receita `cancelled` | 328252.48 |
| Receita `refunded` | 260335.23 |
| Receita `created` | 279216.30 |

A reconciliação demonstrou:

```text
analytics.revenue_by_month
= 1.416.127,23

gold.fct_sales
= 1.416.127,23

```

Portanto, não existe divergência matemática entre a Gold e a tabela Analytics.

A camada Analytics está reproduzindo corretamente a soma de `net_amount` existente na `fct_sales`.

## Cenário investigativo

Como exercício de impacto, foi calculado um cenário considerando apenas pedidos com status:

```text
paid
shipped
```

Esse cenário produziu:

```text
Receita atual publicada
R$ 1.416.127,23

Receita paid + shipped
R$   548.323,22

Diferença
R$   867.804,01
```

A diferença corresponde a aproximadamente 61,28% do valor atualmente publicado como receita.

Esse valor NÃO deve ser interpretado como receita comprovadamente incorreta.

O filtro `paid + shipped` foi utilizado somente como cenário investigativo, pois não foi encontrada documentação no projeto determinando formalmente que esses sejam os únicos status elegíveis para receita.

## Análise da implementação Analytics

A consulta `sql/analytics/revenue_by_month.sql` utiliza:

```sql
SUM(f.net_amount) AS revenue_net
```

sobre `gold.fct_sales`.

Não existe filtro explícito do tipo:

```sql
WHERE order_status IN (...)
```

Portanto, todos os registros existentes na `fct_sales` participam da agregação de receita.

A investigação identificou comportamento semelhante em outras consultas analíticas que utilizam métricas financeiras derivadas da `fct_sales`:

- `revenue_by_month.sql`;
- `revenue_by_category.sql`;
- `top_customers.sql`;
- `campaign_performance.sql`.

Isso aumenta o possível raio de impacto de qualquer futura definição de elegibilidade de receita.

## Análise da documentação

Foi realizada busca por referências a:

```text
revenue
receita
order_status
cancelled
refunded
paid
shipped
created
```

nos arquivos de documentação e SQL do projeto.

A documentação descreve métricas de receita por mês, categoria, cliente e campanha.

Também existem regras de Data Quality que garantem:

- status pertencente à lista permitida;
- quantidade válida;
- `net_amount` adequado para agregação;
- integridade das tabelas Gold.

Entretanto, não foi encontrada uma definição explícita determinando quais `order_status` devem participar das métricas denominadas `revenue`.

## Análise dos testes automatizados

Os testes existentes validam corretamente comportamentos técnicos do pipeline.

Entre eles:

- normalização de status;
- tratamento de status inválidos;
- geração da `fct_sales`;
- preservação de `order_status`;
- cálculo de `net_amount`;
- materialização das tabelas Analytics.

Entretanto, não foi identificado teste automatizado que estabeleça uma regra como:

```text
cancelled não deve compor receita
```

ou:

```text
somente paid e shipped são elegíveis para receita
```

Portanto, os testes atuais também não constituem um contrato de negócio para reconhecimento de receita.

## Causa raiz

Não foi identificado defeito técnico na agregação.

A camada Analytics executa corretamente a regra atualmente implementada:

```text
revenue = SUM(fct_sales.net_amount)
```

O problema identificado é a ausência de uma definição semântica explícita para a métrica `revenue`.

Atualmente o projeto não diferencia formalmente conceitos como:

```text
booked revenue
paid revenue
recognized revenue
realized revenue
```

Como consequência, pedidos `created`, `cancelled` e `refunded` podem contribuir para métricas chamadas genericamente de receita sem que exista documentação justificando esse comportamento.

## Classificação

A investigação classifica o DE-102 como:

```text
Tipo:
lacuna de requisito / contrato semântico

Integridade dos dados:
sem problema identificado

Reconciliação Gold versus Analytics:
consistente

Defeito técnico:
não comprovado

Risco:
métricas financeiras podem possuir interpretação ambígua

Impacto potencial:
múltiplas tabelas do schema analytics
```

## Recomendação

Não é recomendada alteração imediata nos SQLs de Analytics antes da definição formal da regra de negócio.

A definição deverá esclarecer, pelo menos, o tratamento dos seguintes estados:

```text
created
paid
shipped
cancelled
refunded
```

Também deverá esclarecer se pedidos reembolsados:

- são excluídos integralmente;
- permanecem como receita histórica;
- são representados como valor negativo;
- são tratados em métrica separada.

Após a definição do contrato de negócio, a implementação deverá utilizar uma regra única e consistente para todas as métricas financeiras.

## Testes recomendados após definição da regra

Quando a regra de receita for formalizada, deverão ser adicionados testes que validem explicitamente:

```text
status elegíveis para receita;
status não elegíveis;
tratamento de refunded;
valor total de receita;
receita por mês;
receita por categoria;
receita por cliente;
receita por campanha.
```

Também é recomendável implementar uma reconciliação automatizada entre a regra oficial de receita na Gold e as tabelas materializadas em Analytics.

## Resultado da investigação

Status: requisito semântico de receita não definido.

Falha técnica comprovada: não.

Inconsistência Gold versus Analytics: não.

Risco identificado: a métrica denominada `revenue` inclui todos os status válidos da `fct_sales`, mas o projeto não documenta quais desses status representam receita reconhecida pelo negócio.

Impacto potencial observado: até R$ 867.804,01 do valor atualmente publicado depende da definição de elegibilidade de status no cenário investigativo analisado.

Próxima ação recomendada: obter definição formal de negócio antes de alterar as consultas SQL ou a modelagem da Gold.
