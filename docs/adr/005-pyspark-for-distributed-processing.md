# ADR 005: Uso de PySpark para Processamento Distribuído Local

## Status

Aceito

## Contexto

O laboratório precisava de uma tecnologia de processamento que representasse, de forma plausível, workloads típicos de engenharia de dados em ecossistemas Lakehouse. A solução também precisava:

- funcionar localmente;
- ser gratuita;
- permitir transformações tabulares em escala moderada;
- suportar leitura e escrita em Parquet;
- manter aderência conceitual ao tipo de engine frequentemente usado em AWS e em plataformas analíticas modernas.

## Decisão

Usar `PySpark` como engine principal de processamento distribuído local para ingestão, transformação, publicação das camadas e benchmark de otimização.

Essa escolha foi feita porque o PySpark:

- representa uma competência reconhecida em engenharia de dados;
- suporta bem workloads batch e modelagem por camadas;
- conversa naturalmente com Parquet e fluxos de data lake;
- permite discutir tuning, joins, shuffle, cache e broadcast;
- mantém boa aderência conceitual ao tipo de processamento distribuído comum em ambientes gerenciados.

## Alternativas consideradas

- `pandas`.
  - Rejeitado por ser menos adequado para a narrativa de processamento distribuído.
- `DuckDB` como engine principal de transformação.
  - Rejeitado para o pipeline principal por não representar tão bem os conceitos de execução distribuída e tuning Spark.
- `Polars`.
  - Rejeitado por ser menos alinhado à proposta de simular competências amplamente esperadas em plataformas de dados corporativas.

## Consequências positivas

- O projeto evidencia domínio de conceitos associados a Spark.
- Os jobs do laboratório ficam coerentes com uma arquitetura Lakehouse moderna.
- O benchmark de performance ganha base técnica relevante.
- O projeto consegue discutir particionamento, AQE, shuffle e layout de arquivos com mais maturidade.

## Trade-offs

- O ambiente local passa a depender de `java` e `pyspark`.
- O tempo de setup e execução é maior do que em soluções puramente locais e in-memory.
- Parte da experiência depende mais da capacidade da máquina do usuário.

## Impacto no projeto

- As camadas Bronze, Silver e Gold passam a ser materializadas por jobs Spark.
- O benchmark de otimização Spark se torna um componente natural do repositório.
- A documentação precisa explicar o uso de PySpark como escolha arquitetural, não apenas implementação.
