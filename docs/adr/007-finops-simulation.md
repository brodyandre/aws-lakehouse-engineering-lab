# ADR 007: Simulação de FinOps Baseada em Arquivos Locais

## Status

Aceito

## Contexto

O laboratório pretende demonstrar que decisões de engenharia de dados afetam custo, mas não pode usar AWS real, billing, APIs de custo ou credenciais. Ainda assim, era importante representar conceitos de FinOps de forma útil para discussão técnica.

A solução precisava:

- funcionar sem custo;
- ser transparente e reproduzível;
- manter vínculo conceitual com S3 e Athena;
- ajudar a discutir Parquet, particionamento e `small files`.

## Decisão

Adotar um modelo de `FinOps simulado`, baseado no tamanho e na quantidade dos arquivos locais das camadas `Raw`, `Bronze`, `Silver` e `Gold`, com estimativas de:

- storage estilo S3;
- scan estilo Athena;
- risco de `small files`;
- economia potencial com Parquet e particionamento.

## Alternativas consideradas

- Integrar com billing real da AWS.
  - Rejeitada por quebrar a premissa de ambiente local e gratuito.
- Não incluir FinOps no projeto.
  - Rejeitada por perder uma dimensão importante de maturidade arquitetural.
- Simular custo apenas por tempo de execução.
  - Rejeitada por não representar bem a relação entre layout físico, scan e armazenamento.

## Consequências positivas

- O projeto passa a discutir custo como atributo de arquitetura.
- A simulação ajuda a explicar por que Parquet, partição e compactação importam.
- O laboratório fica mais completo para avaliação técnica e documentação.
- O modelo é legível e ajustável sem dependência externa.

## Trade-offs

- Os números não representam billing real.
- Custos como rede, requests, classes de armazenamento e múltiplos serviços não são modelados.
- O valor principal da solução está no raciocínio comparativo, não na precisão financeira.

## Impacto no projeto

- FinOps se torna um pilar explícito do laboratório.
- Relatórios locais passam a representar também governança de custo.
- A documentação precisa deixar claro que a solução é uma simulação arquitetural, não um espelho fiel de cobrança em produção.
