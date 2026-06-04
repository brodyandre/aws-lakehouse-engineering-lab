# Observabilidade de dados

## Objetivo

Observabilidade de dados é a capacidade de entender o comportamento operacional do pipeline: o que executou, quanto tempo levou, quanto processou, quais artefatos gerou e se houve sinais de risco para consumo analítico.

No contexto deste laboratório, a observabilidade foi desenhada para ser:

- local;
- gratuita;
- reproduzível;
- útil como evidência técnica.

## Log, métrica e alerta

| Conceito | O que significa | Exemplo no projeto |
| --- | --- | --- |
| Log | evento textual de execução | início de job, linhas lidas, caminho de saída |
| Métrica | medida numérica consolidada | duração, registros de entrada, inválidos, tamanho de artefatos |
| Alerta | reação automática a condição anormal | ainda não integrado a notificações externas, mas preparado conceitualmente |

## O que o projeto mede

Os módulos [src/observability/metrics_collector.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/src/observability/metrics_collector.py) e [src/observability/pipeline_monitor.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/src/observability/pipeline_monitor.py) consolidam, por execução:

| Métrica | Descrição |
| --- | --- |
| `job_name` | nome do job executado |
| `started_at` | início da execução |
| `finished_at` | fim da execução |
| `duration_seconds` | duração total |
| `source_layer` | camada de origem |
| `target_layer` | camada de destino |
| `records_in` | volume de entrada |
| `records_out` | volume de saída |
| `invalid_records` | registros com problemas relevantes |
| `valid_data_percentage` | percentual aproximado de dados válidos |
| `generated_files` | artefatos produzidos |
| `approx_file_size_bytes` | tamanho aproximado dos arquivos |
| `status` | `success`, `warning` ou `failed` |

## Artefatos gerados

| Arquivo | Papel |
| --- | --- |
| `reports/observability/pipeline_metrics.json` | histórico estruturado de execuções |
| `reports/observability/pipeline_metrics.md` | visão legível para documentação e troubleshooting |

## Como a observabilidade entra no pipeline

Os jobs principais já registram métricas automaticamente:

- [spark/jobs/raw_to_bronze.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/spark/jobs/raw_to_bronze.py)
- [spark/jobs/bronze_to_silver.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/spark/jobs/bronze_to_silver.py)
- [spark/jobs/silver_to_gold.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/spark/jobs/silver_to_gold.py)

O comportamento esperado é:

- o job executa;
- o relatório funcional da etapa é gerado;
- a execução também atualiza o histórico consolidado de observabilidade;
- em caso de falha, o sistema tenta registrar status `failed` antes de propagar o erro.

## Simulação local versus ambiente corporativo

| Neste laboratório | Em um ambiente corporativo |
| --- | --- |
| relatórios Markdown e JSON | dashboards, APM e métricas centralizadas |
| logs locais de container e terminal | CloudWatch, Datadog, ELK, Splunk |
| histórico simples de execução | telemetria contínua e alertas |
| leitura manual de evidências | monitores e SLOs automatizados |

## Como isso seria levado para AWS CloudWatch

Em AWS real, uma evolução natural seria:

- enviar logs estruturados para `CloudWatch Logs`;
- publicar métricas customizadas em `CloudWatch Metrics`;
- configurar `CloudWatch Alarms` para falhas, latência ou perda de qualidade;
- usar dashboards para acompanhar SLA operacional.

Exemplos de gatilhos:

- job com `status=failed`;
- aumento brusco de `duration_seconds`;
- queda de `valid_data_percentage`;
- volume de saída abaixo do esperado.

## Como isso seria levado para Datadog ou Prometheus

### Datadog

Seria adequado para:

- centralizar logs;
- publicar métricas por pipeline e camada;
- criar monitores e dashboards operacionais;
- correlacionar performance do pipeline com infraestrutura.

### Prometheus

Seria adequado para:

- expor métricas scrapeáveis;
- criar séries temporais;
- alimentar dashboards em Grafana;
- usar Alertmanager para notificações.

## Benefício técnico no projeto

Observabilidade agrega valor porque o projeto deixa de ser apenas um conjunto de scripts e passa a demonstrar:

- visão operacional;
- preocupação com suporte e troubleshooting;
- geração de evidência auditável;
- maturidade para discutir execução além de transformação.

## Limites da abordagem atual

Esta implementação não tenta reproduzir:

- tracing distribuído completo;
- alertas externos reais;
- retenção de logs enterprise;
- correlação com infraestrutura de nuvem em tempo real.

O objetivo é mostrar fundamentos sólidos de observabilidade em uma arquitetura Lakehouse local e sem custo.
