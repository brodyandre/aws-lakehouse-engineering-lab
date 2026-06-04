# Jobs Spark

Este diretório armazenará os jobs PySpark responsáveis por:

- ingestão técnica de dados para Bronze;
- conformidade e limpeza para Silver;
- publicação de fatos, dimensões e data marts em Gold;
- geração de métricas operacionais e artefatos auxiliares.

## Convenções sugeridas

- um arquivo por etapa principal de transformação;
- parâmetros de execução explícitos;
- escrita idempotente por partição ou snapshot;
- logs suficientes para troubleshooting local.
