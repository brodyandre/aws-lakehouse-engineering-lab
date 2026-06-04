# ADR 006: Estratégia de Data Quality com Regras Automatizadas e Artefatos Locais

## Status

Aceito

## Contexto

O projeto precisava demonstrar que transformação de dados não é suficiente sem mecanismos explícitos de confiança analítica. Ao mesmo tempo, a estratégia de Data Quality deveria:

- funcionar localmente;
- evitar dependência de serviços pagos;
- ser transparente para leitura técnica;
- gerar evidências reutilizáveis em pipeline e CI/CD;
- permanecer simples o suficiente para manutenção em um laboratório.

## Decisão

Adotar uma estratégia de Data Quality baseada em regras explícitas implementadas em Python e PySpark, com validação sobre as camadas Silver e Gold e geração de artefatos em Markdown e JSON.

A decisão inclui:

- checagens de completude, validade, consistência e unicidade;
- execução local como script;
- integração com a DAG do Airflow;
- testes automatizados específicos para Data Quality;
- persistência de resultados para leitura humana e automação.

## Alternativas consideradas

- `Great Expectations`.
  - Rejeitado por adicionar mais complexidade e convenção do que o necessário ao escopo atual.
- Validar apenas via asserts em testes unitários.
  - Rejeitado por reduzir observabilidade operacional e dificultar uso dentro do pipeline.
- Não ter uma camada dedicada de qualidade.
  - Rejeitado por empobrecer a maturidade arquitetural do projeto.

## Consequências positivas

- A qualidade de dados passa a ser evidência concreta do pipeline.
- O projeto demonstra preocupação com confiabilidade analítica.
- Os resultados podem ser consumidos tanto por pessoas quanto por automação.
- A camada Silver e a Gold ganham contratos mínimos mais claros.

## Trade-offs

- A implementação é mais manual do que frameworks especializados.
- O conjunto de regras ainda depende de manutenção explícita conforme o modelo evolui.
- O laboratório não cobre, neste momento, um catálogo enterprise de regras.

## Impacto no projeto

- Data Quality deixa de ser apenas discurso e vira etapa formal do pipeline.
- O CI/CD passa a poder validar consistência analítica mínima.
- A documentação técnica precisa explicar o papel da qualidade na confiança da Gold.
