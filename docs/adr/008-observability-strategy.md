# ADR 008: Estratégia de Observabilidade com Logs, Métricas e Relatórios Locais

## Status

Aceito

## Contexto

O laboratório precisava demonstrar capacidade operacional além da simples execução de jobs. Era importante responder perguntas como:

- qual job executou;
- quanto tempo levou;
- quantos registros processou;
- quais artefatos gerou;
- se a execução terminou com sucesso, warning ou falha.

Ao mesmo tempo, a solução deveria continuar:

- local;
- gratuita;
- simples de operar;
- útil para leitura humana e automação.

## Decisão

Adotar uma estratégia de observabilidade local composta por:

- logs padronizados;
- métricas consolidadas por execução;
- persistência em `JSON` e `Markdown`;
- integração direta com os jobs principais do pipeline.

Essa abordagem gera evidências operacionais sem depender de CloudWatch, Datadog, Prometheus ou outros serviços externos.

## Alternativas consideradas

- Integrar desde o início com stack completa de Prometheus e Grafana.
  - Rejeitada por exceder o escopo e a simplicidade desejada para o laboratório.
- Usar apenas logs no terminal.
  - Rejeitada por não oferecer histórico consolidado nem artefatos reutilizáveis.
- Integrar com SaaS de observabilidade.
  - Rejeitada por custo, complexidade e dependência externa.

## Consequências positivas

- O projeto passa a gerar rastros operacionais claros.
- A observabilidade pode ser discutida como parte da arquitetura, não como detalhe posterior.
- O histórico em JSON e Markdown serve como evidência profissional.
- Os jobs ganham maior transparência de volume, status e artefatos.

## Trade-offs

- Não há alertas reais integrados com canais externos.
- O modelo não cobre tracing distribuído ou telemetria corporativa avançada.
- Parte da análise continua sendo manual.

## Impacto no projeto

- Os jobs principais passam a registrar métricas automaticamente.
- A documentação técnica precisa explicar a diferença entre logs, métricas e alertas.
- O projeto se aproxima mais de uma visão operacional madura, mesmo em ambiente local.
