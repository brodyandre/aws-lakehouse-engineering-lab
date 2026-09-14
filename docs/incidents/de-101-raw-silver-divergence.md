# DE-101 — Divergência de registros entre Raw, Silver e Gold

- Issue: #2
- Branch: `investigation/de-101-raw-silver-divergence`
- Data da investigação: 2026-09-14
- Status: comportamento esperado pela implementação atual

## Contexto

Durante a validação do pipeline Lakehouse foi identificada uma possível divergência na quantidade de registros entre as camadas Raw, Bronze, Silver e Gold, principalmente nas entidades `orders` e `order_items`.

A suspeita inicial era de que registros poderiam estar sendo perdidos durante as transformações do pipeline. Como o processamento terminava sem falhas aparentes, foi necessário analisar as contagens por camada, os relatórios de Data Quality e as regras aplicadas durante a transformação dos dados.

A investigação foi realizada sem modificar o código ou alterar manualmente os datasets.

## Evidências coletadas

### Contagem Raw, Bronze e Silver

As primeiras validações compararam diretamente a quantidade de registros das entidades `orders` e `order_items`.

| Entity      |  Raw | Bronze | Silver |
| ----------- | ---: | -----: | -----: |
| orders      |  400 |    400 |    400 |
| order_items | 1000 |   1000 |   1000 |

Não foi observada redução na quantidade de registros entre Raw, Bronze e Silver.

Isso indica que as transformações dessas camadas não estão descartando registros dessas duas entidades durante o fluxo analisado.

### Registros inválidos na Silver

O relatório de transformação Bronze para Silver apresentou registros classificados como inválidos:

| Entity      | Records | Invalid | Quality |
| ----------- | ------: | ------: | ------: |
| orders      |     400 |      12 |     97% |
| order_items |    1000 |      30 |     97% |

Apesar da existência desses registros inválidos, as quantidades totais permaneceram iguais.

No caso de `orders`, os registros com status fora da lista permitida são mantidos na Silver. O campo `is_valid_status` recebe valor `false` e o `order_status` tratado passa a ser `null`.

No caso de `order_items`, registros com quantidade não positiva também permanecem na Silver, sendo identificados através da coluna `is_quantity_valid`.

Portanto, um registro inválido não representa necessariamente um registro descartado. A estratégia utilizada nessa camada é preservar os dados e sinalizar problemas de qualidade para manter rastreabilidade.

### Data Quality

O processo de Data Quality executou 15 verificações e todas foram concluídas com sucesso.

Isso inicialmente poderia parecer contraditório, já que o relatório Bronze para Silver identificou registros inválidos.

Entretanto, as regras de Data Quality verificam se os registros estão sendo tratados de acordo com o comportamento esperado da Silver.

Por exemplo, um pedido com status inválido pode estar corretamente representado como:

* `is_valid_status = false`;
* `order_status = null`.

Nesse caso, o dado de origem continua sendo considerado inválido, mas seu tratamento na Silver está correto.

Dessa forma, o resultado de 100% em uma regra de Data Quality não significa necessariamente que todos os dados recebidos eram válidos, mas que os dados estão consistentes com as regras de tratamento implementadas.

### Rastreamento Silver para Gold

Durante a investigação foi identificada redução de registros entre `order_items` na Silver e `fct_sales` na Gold.

| Stage                   | Records |
| ----------------------- | ------: |
| Silver order_items      |    1000 |
| Valid order_items       |     970 |
| Items JOIN valid orders |     937 |
| After customer JOIN     |     937 |
| After product JOIN      |     937 |
| Gold fct_sales          |     937 |

A primeira redução ocorre na filtragem dos itens válidos:

`1000 -> 970`

Foram removidos 30 registros classificados com `is_quantity_valid = false`.

A segunda redução ocorre durante o relacionamento entre itens válidos e pedidos válidos:

`970 -> 937`

Foram identificados 33 `order_items` associados a pedidos considerados inválidos.

Esses 33 itens pertencem a 12 pedidos distintos.

Todos os 12 pedidos possuem `order_date` preenchida, mas apresentam:

* `is_valid_status = false`;
* `order_status = null`.

Após essa etapa, os joins com clientes e produtos não provocaram novas reduções, mantendo a quantidade em 937 registros até a geração da `fct_sales`.

## Causa raiz

A diferença entre os 1000 registros de `order_items` existentes na Silver e os 937 registros presentes na `fct_sales` é explicada pelas regras de qualidade utilizadas na construção da camada Gold.

A redução total de 63 registros é composta por:

* 30 `order_items` removidos por quantidade inválida;
* 33 `order_items` associados a 12 pedidos com status inválido.

A relação pode ser representada da seguinte forma:

```text
1000 Silver order_items
 -30 itens com quantidade inválida
 -33 itens associados a pedidos inválidos
-------------------------------------------
 937 registros na Gold fct_sales
```

Não foram identificadas perdas adicionais nos joins com clientes ou produtos.

Também não foi encontrada evidência de que datas nulas fossem responsáveis pela redução dos 33 registros relacionados a pedidos inválidos.

## Conclusão

A investigação não encontrou evidência de perda inesperada de registros entre Raw, Bronze e Silver.

Os registros considerados inválidos são preservados na Silver e identificados através de flags de qualidade.

A redução observada entre Silver e Gold ocorre durante a aplicação de regras que selecionam apenas registros válidos para a construção da tabela `fct_sales`.

Com base no comportamento observado e nas regras atualmente implementadas, a divergência analisada é compatível com o comportamento esperado do pipeline e não indica, até o momento, um defeito de transformação.

Caso o requisito de negócio determine que todos os registros da Silver devam obrigatoriamente aparecer na Gold, então o comportamento deverá ser reavaliado como uma possível divergência entre a implementação atual e o requisito esperado.

## Recomendação

Não é recomendada uma alteração imediata na lógica de transformação.

Como melhoria operacional, seria útil tornar mais explícitas as métricas de rejeição entre Silver e Gold.

O pipeline poderia registrar separadamente informações como:

* quantidade de itens rejeitados por quantidade inválida;
* quantidade de pedidos rejeitados por status inválido;
* quantidade de `order_items` impactados por pedidos inválidos;
* registros descartados em cada etapa de join;
* percentual de aproveitamento entre Silver e Gold.

Essas métricas facilitariam futuras investigações e ajudariam a diferenciar rapidamente uma redução esperada por regra de negócio de uma perda inesperada causada por falha no pipeline.

## Resultado da investigação

Status: comportamento esperado pela implementação atual.

Impacto identificado: redução controlada de 63 registros entre `order_items` Silver e `fct_sales` Gold.

Necessidade de correção imediata: não identificada.

Próxima ação recomendada: melhorar a observabilidade das rejeições e documentar explicitamente as regras de elegibilidade para publicação na camada Gold.
