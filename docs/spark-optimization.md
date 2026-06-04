# Otimização com Spark

## Objetivo

Este documento descreve como o projeto demonstra tuning básico de Spark em ambiente local, usando um benchmark comparativo entre uma abordagem funcional, porém ingênua, e outra ajustada com boas práticas de performance.

O benchmark principal está em [spark/benchmarks/spark_optimization_benchmark.py](/home/luizandre/aula/aws-lakehouse-engineering-lab/spark/benchmarks/spark_optimization_benchmark.py).

## Cenário analisado

O benchmark usa dados da camada Gold para calcular:

- receita por categoria;
- receita por mês;
- receita por campanha;
- top clientes.

Essas consultas foram escolhidas porque representam agregações comuns em ambientes analíticos e exigem joins entre fatos e dimensões.

## Comparação entre as versões

| Aspecto | Não otimizada | Otimizada |
| --- | --- | --- |
| Reuso de DataFrame | sem cache | cache do fato de vendas |
| Join com dimensões pequenas | join direto | `broadcast join` |
| Leitura | mais ampla | `column pruning` |
| Shuffle partitions | alto e genérico | ajustado ao laboratório |
| Distribuição de dados | sem planejamento explícito | `repartition` por chave analítica |
| Saídas pequenas | sem ajuste | `coalesce` |
| Adaptive Query Execution | desligado | ligado |

## O que a versão não otimizada representa

Ela simula um cenário comum de início de projeto:

- o pipeline funciona;
- os resultados estão corretos;
- mas o plano de execução ainda não foi ajustado para custo e performance.

Esse tipo de baseline é útil porque mostra que otimização deve ser medida, não presumida.

## O que a versão otimizada demonstra

Ela mostra como pequenas decisões técnicas podem melhorar execução:

- reutilizar o fato de vendas quando várias consultas leem a mesma base;
- diminuir shuffle em joins fato-dimensão com `broadcast`;
- evitar leitura de colunas desnecessárias;
- ajustar partições à escala local do projeto;
- reduzir partições de resultados pequenos antes da materialização final.

## Técnicas aplicadas

### Cache

| Quando usar | Quando evitar |
| --- | --- |
| quando o mesmo DataFrame participa de várias ações | quando o dataset é usado uma única vez |
| quando recomputar é caro | quando a memória local é limitada |
| quando há ganho claro de reúso | quando o dataset é grande demais para o ambiente |

### Broadcast Join

| Quando usar | Quando evitar |
| --- | --- |
| dimensões pequenas | dimensões que podem crescer e pressionar memória |
| joins fato-dimensão frequentes | tabelas cujo tamanho real não está claro |
| redução de shuffle | cenários onde o broadcast não cabe confortavelmente em memória |

### Shuffle Partitions

| Quando usar ajuste manual | Quando evitar excesso de ajuste |
| --- | --- |
| volumes locais menores que o padrão do Spark | quando a redução cria poucas tasks e gargalo |
| cluster pequeno ou local | quando o volume cresce e exige mais paralelismo |
| overhead de tasks excessivas | quando a mudança não foi medida |

### Repartition

| Quando usar | Quando evitar |
| --- | --- |
| antes de agregações ou joins amplos | quando só adiciona shuffle sem benefício claro |
| para redistribuir dados por chave relevante | quando o dado já está bem distribuído |

### Coalesce

| Quando usar | Quando evitar |
| --- | --- |
| em saídas pequenas | antes de transformações pesadas |
| para reduzir quantidade de partições e arquivos | quando concentrar trabalho em poucas tasks prejudica a execução |

### Adaptive Query Execution

| Quando usar | Quando evitar confiança excessiva |
| --- | --- |
| em workloads analíticos com volume variável | quando o plano base já está mal desenhado |
| para deixar o Spark reagir melhor ao tamanho real das partições | quando há skew ou small files que exigem tratamento adicional |

## Como executar

```bash
python3 spark/benchmarks/spark_optimization_benchmark.py \
  --gold-dir data/gold \
  --report-path reports/pipeline_runs/spark_optimization_benchmark.md \
  --master local[*]
```

## O que o relatório mostra

O relatório gerado em `reports/pipeline_runs/spark_optimization_benchmark.md` resume:

- tempo total da versão não otimizada;
- tempo total da versão otimizada;
- diferença percentual;
- tempo por consulta;
- técnicas aplicadas;
- observações de uso e trade-offs.

## Como interpretar os resultados

O benchmark é local. Por isso, o valor absoluto do tempo depende de:

- CPU;
- memória;
- disco;
- carga concorrente da máquina;
- volume materializado na camada Gold.

O foco correto é comparar tendências entre as abordagens, não prometer performance absoluta.

## Relação com o restante do projeto

O benchmark complementa:

- a modelagem Gold, porque usa fatos e dimensões reais do laboratório;
- a observabilidade, porque reforça leitura de métricas operacionais;
- o FinOps, porque performance e volume escaneado afetam custo;
- a narrativa técnica do projeto, mostrando que a preocupação não termina na transformação funcional.

## Limites da simulação

Este benchmark não pretende substituir:

- profiling de produção;
- análise detalhada de cluster;
- observabilidade distribuída real;
- tuning específico para grandes volumes em nuvem.

Ele existe para demonstrar raciocínio de otimização Spark em um ambiente controlado e sem custo.
