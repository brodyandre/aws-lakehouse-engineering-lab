# Relatórios de FinOps

Esta pasta armazena os artefatos gerados pelo estimador local de custo do laboratório.

Arquivos esperados:

- `cost_estimation.md`
- `cost_estimation.json`

Eles são produzidos por [src/finops/cost_estimator.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/src/finops/cost_estimator.py) e resumem:

- volume por camada;
- quantidade de arquivos;
- tamanho médio;
- indícios de `small files`;
- custo estimado de armazenamento estilo S3;
- custo estimado de leitura estilo Athena;
- economia simulada com Parquet e particionamento.

Exemplo de execução:

```bash
python3 src/finops/cost_estimator.py
```
