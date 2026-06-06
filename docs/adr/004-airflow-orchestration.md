# ADR 004: Uso do Apache Airflow para Orquestração

## Status

Aceito

## Contexto

O laboratório precisava de uma solução de orquestração capaz de representar:

- dependências entre etapas;
- retries;
- execução manual e agendada;
- histórico operacional;
- visibilidade de pipeline ponta a ponta.

Além disso, a ferramenta escolhida deveria ser reconhecida no mercado e compatível com uma execução local e gratuita.

## Decisão

Usar `Apache Airflow` como orquestrador principal do projeto, com DAGs executadas localmente em Docker e uma DAG central para o pipeline Lakehouse.

Na evolução atual do laboratório, a implementação foi atualizada para `Airflow 3`, separando os apps `core` e `execution` em containers distintos e adicionando o `dag-processor` como componente explícito da stack.

O Airflow foi escolhido para:

- representar um padrão realista de orquestração em dados;
- permitir desenho explícito de dependências;
- exibir status operacional de forma familiar para times técnicos;
- integrar-se bem a scripts Python e jobs Spark existentes no repositório.

## Alternativas consideradas

- `cron` e scripts shell.
  - Rejeitados por não oferecerem visibilidade adequada de dependências e histórico.
- `Prefect`.
  - Rejeitado por não ser a melhor escolha para a narrativa arquitetural atual do laboratório.
- `Dagster`.
  - Rejeitado por adicionar um modelo operacional e conceitual mais pesado para o escopo.

## Consequências positivas

- O pipeline ganha uma representação operacional clara.
- O projeto demonstra entendimento de orquestração além de scripts isolados.
- Retries, ordem de execução e histórico passam a ser parte visível da solução.
- A DAG reforça a leitura do projeto como arquitetura, não apenas como coleção de jobs.
- A topologia do Airflow fica mais próxima de uma arquitetura moderna orientada a serviços.

## Trade-offs

- O Airflow adiciona peso ao ambiente local.
- O laboratório passa a exigir mais atenção com containers e metadados locais.
- Parte do ganho pedagógico vem com maior custo de setup em comparação a simples scripts.

## Impacto no projeto

- A documentação de arquitetura precisa explicar a função da DAG principal.
- O Compose inclui serviços adicionais como Postgres e componentes do Airflow.
- A documentação passa a explicar a separação entre `core API`, `execution API`, `scheduler`, `triggerer` e `dag-processor`.
- A narrativa do projeto passa a incluir não apenas transformação, mas também operação de pipeline.
