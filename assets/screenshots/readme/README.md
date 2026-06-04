# Screenshots do README

Esta pasta organiza as capturas usadas no `README.md` por tema, para facilitar manutenção local e navegação no GitHub.

## Estrutura

```text
assets/screenshots/readme/
├── architecture/
├── cicd/
├── data-quality/
├── finops/
├── modeling/
├── observability/
├── orchestration/
└── runtime/
```

## Convenção

- `architecture/`: visão geral da arquitetura do laboratório.
- `modeling/`: camada `Gold` e modelagem analítica.
- `data-quality/`: relatórios e evidências de qualidade.
- `observability/`: métricas operacionais e monitoramento.
- `finops/`: relatórios de custo simulado.
- `orchestration/`: DAG do Airflow e execuções do pipeline.
- `cicd/`: workflows e checks do GitHub Actions.
- `runtime/`: stack local e serviços em execução.

## Boas práticas

- manter nomes numerados para preservar a ordem narrativa do `README`;
- usar `PNG` com recorte limpo e boa legibilidade;
- atualizar o `README.md` sempre que um arquivo for movido ou renomeado.
