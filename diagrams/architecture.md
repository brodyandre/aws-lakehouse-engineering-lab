# Diagrama de arquitetura

```mermaid
flowchart TB
    subgraph Source["Entrada e preparação"]
        S1["Gerador sintético"]
        S2["data/raw<br/>CSV e JSON"]
        S1 --> S2
    end

    subgraph Storage["Persistência local"]
        M1["MinIO<br/>simulação de S3"]
        M2["Parquet local"]
    end

    subgraph Orchestration["Orquestração"]
        O1["Apache Airflow<br/>lakehouse_pipeline_dag"]
    end

    subgraph Processing["Processamento PySpark"]
        P1["raw_to_bronze.py"]
        P2["bronze_to_silver.py"]
        P3["silver_to_gold.py"]
        P4["spark_optimization_benchmark.py"]
    end

    subgraph Layers["Camadas do Lakehouse"]
        L1["Raw"]
        L2["Bronze"]
        L3["Silver"]
        L4["Gold"]
    end

    subgraph GoldModel["Modelo analítico"]
        G1["dim_customer"]
        G2["dim_product"]
        G3["dim_campaign"]
        G4["dim_date"]
        G5["fct_sales"]
        G6["fct_web_events"]
    end

    subgraph Reliability["Confiabilidade e operação"]
        R1["Data Quality"]
        R2["Observabilidade"]
        R3["FinOps simulado"]
        R4["GitHub Actions"]
    end

    subgraph Consumption["Consumo"]
        C1["SQL analítico"]
        C2["Data marts"]
        C3["Relatórios locais"]
    end

    O1 --> S1
    S2 --> L1
    L1 --> P1
    P1 --> L2
    L2 --> P2
    P2 --> L3
    L3 --> P3
    P3 --> L4

    L1 --> M1
    L2 --> M2
    L3 --> M2
    L4 --> M2
    M1 -. referência conceitual .- M2

    L4 --> G1
    L4 --> G2
    L4 --> G3
    L4 --> G4
    L4 --> G5
    L4 --> G6

    G1 --> C1
    G2 --> C1
    G3 --> C1
    G4 --> C1
    G5 --> C1
    G6 --> C1
    C1 --> C2
    C1 --> C3

    L3 --> R1
    L4 --> R1
    P1 --> R2
    P2 --> R2
    P3 --> R2
    L2 --> R3
    L3 --> R3
    L4 --> R3
    R4 --> R1
    R4 --> P1
    R4 --> P2
    R4 --> P3

    L4 --> P4
    P4 --> C3
```

## Leitura rápida

- `MinIO` representa o papel conceitual do `S3`.
- `Raw`, `Bronze`, `Silver` e `Gold` estruturam a evolução do dado.
- `PySpark` move os dados entre camadas.
- `Airflow` orquestra o pipeline ponta a ponta.
- `Data Quality`, `Observabilidade` e `FinOps` geram evidências operacionais.
- `GitHub Actions` valida qualidade técnica do repositório.
