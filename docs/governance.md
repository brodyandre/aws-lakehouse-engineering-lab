# Governança de dados

## Objetivo

Governança, neste laboratório, significa tornar o fluxo de dados compreensível, rastreável e previsível. Mesmo sendo um projeto local e orientado a estudo, a governança ajuda a responder perguntas importantes:

- o que cada dataset representa;
- em qual camada ele pertence;
- quais regras foram aplicadas;
- quais evidências sustentam a confiabilidade da entrega;
- como mudanças de schema e comportamento devem ser tratadas.

## Princípios de governança

| Princípio | Aplicação no projeto |
| --- | --- |
| Clareza de responsabilidade | cada camada tem propósito explícito |
| Rastreabilidade | jobs e relatórios permitem seguir a lineage básica |
| Padronização | nomenclatura, estrutura de pastas e artefatos seguem convenções estáveis |
| Reprodutibilidade | o pipeline roda localmente de forma controlada |
| Qualidade verificável | a Silver e a Gold possuem validações automáticas |
| Documentação mínima obrigatória | a arquitetura e os módulos principais são documentados |

## Organização por camadas

| Camada | Papel de governança |
| --- | --- |
| `Raw` | preservar a origem e servir como referência do dado de entrada |
| `Bronze` | adicionar metadados técnicos e padronização estrutural |
| `Silver` | consolidar contratos, tipagem e conformidade mínima |
| `Gold` | expor dados analíticos para consumo e métricas de negócio |

## Convenções de nomenclatura

| Tipo | Convenção |
| --- | --- |
| Dados brutos | nomes próximos da origem |
| Dimensões | prefixo `dim_` |
| Fatos | prefixo `fct_` |
| Data marts | diretório `sql/data_marts/` |
| Relatórios de execução | `reports/pipeline_runs/` |
| Relatórios de qualidade | `reports/data_quality/` |
| Relatórios operacionais | `reports/observability/` e `reports/finops/` |

## Lineage e rastreabilidade

O projeto busca responder, de forma simples, a quatro perguntas:

1. qual foi a origem do dado;
2. qual job produziu a próxima camada;
3. quando a execução aconteceu;
4. quais regras de qualidade e transformação foram aplicadas.

Mecanismos usados:

- colunas técnicas como `ingestion_timestamp`, `source_file` e `processing_date`;
- relatórios Markdown e JSON por etapa;
- DAG do Airflow para dependências e histórico;
- documentação das camadas e do modelo Gold.

## Contratos e evolução de schema

A camada Silver funciona como ponto principal de estabilização semântica. É nela que tipos, formatos e regras mínimas de consistência passam a ser tratados de forma mais explícita.

Diretrizes:

- mudanças incompatíveis devem ser documentadas;
- tabelas Gold não devem mudar de forma abrupta sem revisão do impacto analítico;
- novos campos devem ser introduzidos com clareza de objetivo;
- decisões relevantes podem ser registradas via ADR.

## Qualidade como mecanismo de governança

Governança não é apenas organização; também é controle de confiabilidade. Por isso, o projeto inclui validações automatizadas em Silver e Gold para:

- nulidade indevida;
- status inválidos;
- chaves ausentes;
- duplicidade indevida;
- consistência mínima para fatos e dimensões.

Isso ajuda a evitar que a camada Gold seja tratada como confiável sem evidências.

## Dados sensíveis e uso de PII

Este laboratório usa dados sintéticos. Ainda assim, ele adota um princípio importante: não trabalhar com dados reais desnecessários quando o objetivo é aprendizado ou demonstração técnica.

Diretrizes:

- priorizar dados sintéticos ou públicos;
- evitar PII real;
- tratar e-mails e identificadores como exemplos simulados;
- não reutilizar credenciais locais fora do laboratório.

## Governança operacional

Além de datasets, o projeto governa também execução e evidências.

Artefatos monitorados:

- relatórios de pipeline;
- métricas de observabilidade;
- relatórios de Data Quality;
- estimativas de FinOps;
- validações de CI/CD.

Isso cria uma base simples, mas profissional, para demonstrar que a entrega de dados inclui operação, não apenas transformação.

## Limites da governança neste laboratório

O projeto não tenta reproduzir uma governança corporativa completa com:

- catálogo corporativo;
- RBAC centralizado;
- classificação automatizada de dados;
- lineage visual enterprise;
- políticas de retenção regulatória.

O foco é demonstrar fundamentos sólidos e decisões coerentes em um ambiente local, sem custo e orientado a boas práticas.
