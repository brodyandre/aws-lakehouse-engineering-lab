# ADR 003: Adoção de Star Schema na Camada Gold

## Status

Aceito

## Contexto

A camada Gold do laboratório precisa demonstrar capacidade analítica, clareza semântica e boa comunicação com públicos técnicos e de negócio. Como o cenário do projeto envolve clientes, produtos, campanhas, pedidos e eventos digitais, era necessário escolher um modelo que equilibrasse:

- simplicidade de consulta;
- boa narrativa para portfólio;
- aderência a práticas reconhecidas de analytics engineering;
- compatibilidade com perguntas de negócio como receita por categoria, mês e campanha.

## Decisão

Adotar `Star Schema` como padrão inicial de modelagem analítica na camada Gold, com dimensões conformadas e fatos centrais para vendas e eventos digitais.

Essa decisão materializa:

- dimensões para cliente, produto, campanha e data;
- fatos para vendas e eventos web;
- uso de chaves substitutas determinísticas;
- consumo orientado a SQL e métricas analíticas.

## Alternativas consideradas

- `Snowflake Schema`.
  - Rejeitado por adicionar normalização extra sem benefício relevante para o escopo atual.
- `Data Vault`.
  - Rejeitado por ser mais adequado a cenários corporativos de integração e historização extensiva.
- Tabela analítica única e desnormalizada.
  - Rejeitada por reduzir clareza de modelagem e demonstrar menos maturidade arquitetural.

## Consequências positivas

- Consultas analíticas ficam mais simples e legíveis.
- O projeto demonstra um padrão amplamente reconhecido em BI e engenharia de dados.
- O modelo facilita benchmarking, validações de qualidade e exemplos de SQL analítico.
- A camada Gold fica mais fácil de explicar em contextos de entrevista e portfólio.

## Trade-offs

- Algumas redundâncias dimensionais são aceitas em favor de simplicidade analítica.
- O modelo não busca cobrir necessidades complexas de historização ou MDM corporativo.
- Evoluções futuras mais sofisticadas podem exigir estratégias adicionais, como SCD ou modelos complementares.

## Impacto no projeto

- A Gold se torna explicitamente orientada a consumo analítico.
- Os artefatos SQL, benchmark Spark e regras de Data Quality passam a girar em torno do modelo dimensional.
- A documentação do projeto precisa explicar a escolha de Star Schema frente a Snowflake e Data Vault.
