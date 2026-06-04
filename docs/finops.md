# FinOps aplicado a dados

## Objetivo

FinOps aplicado a dados é a prática de medir e explicar impacto financeiro de decisões técnicas em plataformas analíticas. Em um Lakehouse, isso significa olhar não apenas para infraestrutura, mas também para:

- volume armazenado;
- formato de arquivo;
- estratégia de particionamento;
- número de arquivos;
- padrão de leitura e scan;
- reprocessamento e desperdício operacional.

Neste projeto, o objetivo é demonstrar esse raciocínio sem usar AWS real e sem gerar custo.

## Abordagem do laboratório

O módulo [src/finops/cost_estimator.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/src/finops/cost_estimator.py) calcula estimativas locais com base apenas no tamanho e na quantidade dos arquivos presentes nas camadas:

- `raw`
- `bronze`
- `silver`
- `gold`

Não há:

- consulta a billing real;
- uso de credenciais AWS;
- chamadas para Cost Explorer;
- dependência de API paga.

## O que é estimado

| Métrica | O que representa |
| --- | --- |
| volume total por camada | proxy de storage |
| quantidade de arquivos | proxy de organização física |
| tamanho médio dos arquivos | proxy de eficiência operacional |
| `small files` | risco de custo e performance |
| storage estilo S3 | custo mensal simulado por GB |
| scan estilo Athena | custo simulado por volume lido |
| economia com Parquet e particionamento | ganho estimado por leitura mais eficiente |

## Como particionamento, Parquet e compressão reduzem custo

### Parquet

Parquet é colunar. Isso reduz custo porque:

- menos colunas precisam ser lidas;
- a compressão tende a ser melhor;
- o volume escaneado em workloads analíticos costuma cair.

### Particionamento

Particionar dados por colunas úteis, como data, pode reduzir custo porque a engine deixa de ler arquivos irrelevantes para uma consulta.

Exemplo conceitual:

- sem particionamento, uma consulta mensal pode ler o dataset inteiro;
- com particionamento por data, ela lê apenas o subconjunto necessário.

### Compressão

Compressão reduz o volume armazenado e ajuda a diminuir I/O em diversos cenários de leitura.

## Por que controlar small files

`Small files` não aumentam apenas desorganização. Eles também podem provocar:

- mais metadata para administrar;
- mais operações de listagem;
- pior planejamento de leitura no Spark;
- mais overhead de abertura e fechamento de arquivos;
- mais tempo de execução.

Por isso o projeto sinaliza risco de `small files` usando:

- quantidade de arquivos;
- tamanho médio;
- proporção de arquivos abaixo de um limiar configurável.

## Fórmula de simulação

O projeto usa parâmetros configuráveis para estimar:

| Componente | Lógica simplificada |
| --- | --- |
| Storage | `volume_em_gb * custo_por_gb_mes` |
| Scan analítico bruto | `volume_em_tb * custo_por_tb_scanned` |
| Scan otimizado | `volume * fator_parquet * fator_partition_pruning` |
| Economia estimada | `scan_bruto - scan_otimizado` |

Essa abordagem não pretende ser billing real. Ela serve para ensinar e comparar cenários.

## Artefatos gerados

| Arquivo | Papel |
| --- | --- |
| `reports/finops/cost_estimation.md` | leitura humana e evidência executiva/técnica |
| `reports/finops/cost_estimation.json` | consumo por scripts e automação |

## Como isso se conecta com arquitetura

FinOps neste laboratório ajuda a explicar por que decisões como:

- particionar a Silver ou a Gold;
- compactar datasets;
- evitar `small files`;
- usar Parquet em vez de formatos mais caros para scan;
- reduzir leituras desnecessárias;

afetam tanto performance quanto custo.

## Como isso seria aplicado na AWS real

Em uma arquitetura real na AWS, esse raciocínio se conectaria a:

| Projeto local | AWS real |
| --- | --- |
| volume por camada | tamanho por bucket e prefixo no S3 |
| scan simulado | custo por TB escaneado no Athena |
| small files local | problemas de layout físico no data lake |
| relatórios locais | dashboards, CUR e Cost Explorer |
| tuning orientado a Parquet | práticas de redução de scan e storage |

Ferramentas reais associadas:

- `Amazon S3`
- `Amazon Athena`
- `AWS Cost Explorer`
- `CloudWatch`
- relatórios de custo e governança corporativa

## Por que o projeto usa simulação local para evitar custo

O projeto foi desenhado para:

- ser reproduzível em notebook ou desktop comum;
- não depender de conta em nuvem;
- não gerar custo financeiro durante estudo;
- demonstrar pensamento de arquitetura e otimização sem billing real.

Essa decisão é importante para acessibilidade do laboratório e também para honestidade técnica: a proposta é simular boas práticas, não fingir operação produtiva em AWS.

## Limites da abordagem

O módulo atual não tenta reproduzir:

- cobrança real por request;
- storage classes do S3;
- custo de rede;
- custo de serviços gerenciados;
- billing enterprise com múltiplas contas.

Mesmo assim, ele cobre o suficiente para demonstrar raciocínio FinOps aplicado a dados em um contexto Lakehouse.
