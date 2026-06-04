# ADR 002: Uso do MinIO como Simulação de S3

## Status

Aceito

## Contexto

Uma arquitetura Lakehouse inspirada em AWS normalmente se apoia em object storage como base para aterrissagem, versionamento lógico por camadas e persistência analítica. No entanto, este projeto não pode usar Amazon S3 real, nem depender de conta, billing ou credenciais AWS.

Era necessário escolher uma tecnologia que permitisse:

- representar a lógica de buckets e prefixos;
- manter compatibilidade conceitual com integrações S3-style;
- funcionar localmente e sem custo;
- ser simples de operar em Docker.

## Decisão

Usar o `MinIO` como object storage local compatível com API S3 para simular a função arquitetural do Amazon S3 no laboratório.

O MinIO foi adotado para:

- representar a semântica de armazenamento por camadas;
- apoiar integração conceitual com Spark e Airflow;
- aproximar o desenho do projeto do padrão comum em ambientes AWS;
- manter operação integralmente local.

## Alternativas consideradas

- Usar apenas diretórios locais sem camada estilo S3.
  - Rejeitada por reduzir o paralelismo conceitual com arquiteturas modernas de data lake.
- Usar LocalStack.
  - Rejeitada por adicionar mais complexidade operacional do que o necessário para o escopo atual.
- Usar AWS S3 real.
  - Rejeitada por custo, credenciais e quebra da proposta local-first.

## Consequências positivas

- O projeto mantém uma narrativa arquitetural próxima da AWS.
- Spark e demais componentes podem operar com configuração semelhante à de workloads S3-compatible.
- A experiência permanece gratuita e portátil.
- Recrutadores e pessoas técnicas conseguem reconhecer mais facilmente o papel do storage no desenho.

## Trade-offs

- O projeto não reproduz recursos gerenciados do S3, como lifecycle policies, billing, IAM e comportamento real de ambiente multi-tenant.
- Parte do paralelismo com AWS continua sendo conceitual, e não equivalente operacionalmente.
- Há sobrecarga local de manter mais um serviço no Compose.

## Impacto no projeto

- O storage deixa de ser apenas um diretório e passa a representar um bloco arquitetural explícito.
- A documentação precisa explicar claramente a equivalência conceitual `MinIO -> S3`.
- As demais decisões de integração com Spark e Airflow passam a considerar endpoint S3-compatible como referência local.
