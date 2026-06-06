# ADR 001: Arquitetura Local-First para o Laboratório Lakehouse

## Status

Aceito

## Contexto

O objetivo do projeto é demonstrar boas práticas de arquitetura Lakehouse sem depender de conta cloud, billing real, credenciais sensíveis ou serviços pagos. Além disso, o laboratório precisa ser reproduzível por profissionais técnicos em uma máquina comum de desenvolvimento.

Havia, portanto, uma necessidade clara de escolher uma abordagem que priorizasse:

- baixo custo de adoção;
- facilidade de execução local;
- capacidade de simular conceitos relevantes da AWS;
- boa legibilidade arquitetural para documentação e estudo;
- independência de infraestrutura externa.

## Decisão

Adotar uma arquitetura `local-first`, executada com Docker Compose e componentes open source, capaz de simular os principais blocos lógicos de um Lakehouse inspirado em AWS sem provisionar serviços reais.

Essa decisão organiza o projeto em torno de:

- armazenamento local com semântica próxima de object storage;
- processamento distribuído local com Spark;
- orquestração local com Airflow;
- camadas explícitas `Raw`, `Bronze`, `Silver` e `Gold`;
- relatórios locais de qualidade, observabilidade e FinOps.

## Alternativas consideradas

- Usar AWS real em pequena escala.
  - Rejeitada por introduzir custo, necessidade de credenciais e dependência de ambiente externo.
- Implementar tudo apenas com scripts locais sem containers.
  - Rejeitada por reduzir reprodutibilidade e dificultar padronização do ambiente.
- Simular somente parte da arquitetura, sem orquestrador e sem object storage.
  - Rejeitada por enfraquecer a narrativa arquitetural do projeto.

## Consequências positivas

- O projeto pode ser executado sem custo de cloud.
- A experiência é reproduzível em ambiente local controlado.
- A arquitetura fica mais acessível para estudo, demonstração e discussões técnicas.
- O laboratório evidencia preocupação com arquitetura completa, não apenas com scripts isolados.

## Trade-offs

- A simulação não reproduz elasticidade real de serviços gerenciados.
- IAM, billing, rede gerenciada e SLAs de cloud não fazem parte do escopo.
- A execução local impõe limites de CPU, memória e disco do ambiente do usuário.

## Impacto no projeto

- Todas as decisões seguintes passam a privilegiar ferramentas que funcionem bem localmente.
- A documentação precisa deixar claro que se trata de uma simulação arquitetural, não de uma operação produtiva em AWS.
- O valor do projeto fica concentrado em raciocínio técnico, organização, automação e evidências operacionais.
