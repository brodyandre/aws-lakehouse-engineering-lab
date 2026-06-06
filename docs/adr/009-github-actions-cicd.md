# ADR 009: Uso de GitHub Actions para CI/CD Público e Sem Dependência de Secrets

## Status

Aceito

## Contexto

O projeto precisava de uma estratégia de CI/CD compatível com:

- repositório público;
- execução sem secrets;
- runners gratuitos do GitHub;
- validação contínua de lint, YAML, testes e qualidade de dados;
- narrativa coerente com um laboratório técnico moderno.

Além disso, a automação deveria reforçar a ideia de reprodutibilidade e cuidado com regressão, sem introduzir dependências pagas ou infraestrutura adicional.

## Decisão

Adotar `GitHub Actions` como plataforma de CI/CD do projeto, com workflows separados por responsabilidade:

- validação de Python;
- validação de YAML e Compose;
- execução de testes;
- pipeline reduzido focado em Data Quality.

Os workflows foram desenhados para funcionar sem secrets e sem integração com AWS real.

## Alternativas consideradas

- Não incluir CI/CD.
  - Rejeitada por reduzir a maturidade percebida do projeto.
- Usar uma pipeline monolítica única.
  - Rejeitada por dificultar leitura, manutenção e feedback rápido.
- Usar outra plataforma de CI.
  - Rejeitada por menor aderência ao objetivo de documentação pública simples e acessível.

## Consequências positivas

- O projeto demonstra preocupação com regressão e qualidade contínua.
- O repositório ganha uma camada adicional de credibilidade técnica.
- A separação por workflow facilita entendimento do que está sendo validado.
- A estratégia continua acessível e sem custo adicional para o laboratório.

## Trade-offs

- O tempo de pipeline pode ser maior por haver workflows distintos.
- Parte dos testes depende de ambiente com Java e PySpark no runner.
- CI pública não substitui validação mais profunda em ambientes de produção.

## Impacto no projeto

- O README passa a exibir badges e reforçar a visão de projeto vivo.
- O pipeline de dados ganha cobertura automatizada mínima e objetiva.
- A documentação de arquitetura e operação passa a incluir CI/CD como parte do desenho técnico do laboratório.
