# Guia das imagens do README

As imagens desta pasta ajudam a apresentar o laboratório de Engenharia de Dados no GitHub. **Uma imagem bonita não equivale, por si só, à evidência de uma execução real.** Este guia identifica o que pode ser confirmado e como substituir imagens demonstrativas por capturas verificáveis.

## Tipos de imagem

| Imagem | Origem conhecida | Cuidados de interpretação |
| --- | --- | --- |
| `architecture/01-readme-architecture-overview.png` | Diagrama de arquitetura publicado no projeto | Explica o desenho da solução; não representa implantação cloud real. |
| `orchestration/06-readme-airflow-dag.png` | O script `scripts/capture_readme_screenshots.cjs` tenta acessar a interface real do Airflow e capturá-la | Conferir se a imagem mostra a DAG e a execução pretendida; a captura não valida automaticamente o estado das tarefas. |
| `observability/04-readme-observability-metrics.png` | Painel HTML gerado a partir de `reports/observability/pipeline_metrics.json` quando disponível | O script contém valores substitutos; validar a existência e a data do relatório de origem. |
| `query/10-readme-trino-query-serving.png` | Painel HTML gerado por Playwright | As linhas de resultado são **predefinidas no código**; não são resultado de uma consulta SQL capturada ao vivo. |
| `runtime/08-readme-local-services-overview.png` | Painel HTML gerado por Playwright | Todos os status `healthy` estão **predefinidos no código**; não são uma checagem atual de saúde dos serviços. |
| Demais imagens (`modeling`, `data-quality`, `finops`, `cicd`, `orchestration/09`) | Arquivos PNG versionados no repositório | A origem exata da captura não foi estabelecida por este gerador. Evitar afirmar resultado operacional sem verificar a tela e o artefato correspondente. |

## Como produzir evidências verificáveis

1. Rode o pipeline, os testes ou a consulta no ambiente local conforme o [README principal](../../../README.md).
2. Capture a **interface ou saída efetiva** (Airflow, Trino, terminal, relatórios ou GitHub Actions); não use um painel estático como comprovação de saúde dos serviços.
3. Inclua na legenda o sistema, a data da execução, o comando ou a identificação do run, além do link para a evidência de origem quando possível.
4. Oculte tokens, senhas, URLs sensíveis e dados pessoais antes de publicar.
5. Mantenha arquivos legíveis, sem recortes que removam informações essenciais para interpretar a imagem.

### Fontes de evidência que acompanham o código

- [Workflows GitHub Actions](https://github.com/brodyandre/aws-lakehouse-engineering-lab/actions): resultados públicos de execução dos checks.
- `reports/data_quality/`, `reports/observability/`, `reports/finops/` e `reports/query/`: relatórios gerados localmente, não necessariamente versionados.

## Organização

```text
assets/screenshots/readme/
├── architecture/
├── cicd/
├── data-quality/
├── finops/
├── modeling/
├── observability/
├── orchestration/
├── query/
├── runtime/
└── README.md
```

**Nota de manutenção:** o script `scripts/capture_readme_screenshots.cjs` pode criar `archive/` para preservar versões anteriores antes de sobrescrever imagens. Reveja sempre conteúdo, data e legenda após regenerar imagens.
