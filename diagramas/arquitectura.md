# Arquitectura de datos — MiMcDonald's Guatemala

Hay dos diagramas:

1. **Arquitectura actual (inferida):** cómo creemos que funciona hoy el sistema, a partir de la
   documentación pública (Fase 1).
2. **Réplica en miniatura:** la arquitectura medallion que construimos en Databricks Free
   Edition para reproducirla.

Convención: **borde sólido = documentado**, **borde punteado = inferido o supuesto**.

---

## 1. Arquitectura actual (inferida)

```mermaid
flowchart LR
    CLI(("Cliente"))

    subgraph REST["Restaurante · 126 en Guatemala"]
        direction TB
        MOS["Mostrador · McCafé<br/>Centro de Postres"]
        AUTO["AutoMac"]
        KIO["Pantallas digitales<br/>(kioscos)"]
        POS["POS del restaurante"]
        MOS --> POS
        AUTO --> POS
        KIO --> POS
    end

    subgraph EXCL["Canales que NO acumulan"]
        direction TB
        TER["Apps de terceros"]
        CC["Call Center · WhatsApp"]
    end

    subgraph DIG["Plataforma digital regional GT/HN"]
        direction TB
        APP["App McDonald's GT<br/>MiMcDonald's + McDelivery"]
        CRM[("Cuentas de clientes")]
        LOY["Motor de lealtad<br/>reglas + ledger de puntos"]
        CAT[("Catálogo de<br/>recompensas")]
    end

    LEG[("Puntos McDelivery<br/>programa anterior")]
    DL[("Data lake · BI<br/>marketing")]

    CLI -- "muestra QR o código" --> MOS
    CLI -- "muestra QR o código" --> AUTO
    CLI -- "muestra QR o código" --> KIO
    CLI -- "pide, consulta saldo, canjea" --> APP
    TER --> POS
    CC --> POS

    POS -. "carga batch diaria ≤ 24 h" .-> LOY
    APP -- "pedidos y canjes" --> LOY
    APP --> CRM
    CRM --> LOY
    CAT --> APP
    LEG -. "migración única · ago-2025" .-> LOY
    LOY -- "saldo y puntos por vencer" --> APP
    LOY -.-> DL

    classDef inferido stroke-dasharray: 5 5
    classDef excluido fill:#f8d7da,stroke:#b02a37,color:#58151c
    class LOY,DL,POS inferido
    class TER,CC excluido
```

**Cómo leerlo:**

- **Dos rutas de entrada.** Las compras en el restaurante pasan por el **POS** y llegan al
  motor de lealtad en lotes: por eso los puntos tardan hasta 24 horas (R9). Las compras en la
  **app** ya conocen al cliente y entran directo.
- **El POS no conoce al cliente.** Solo recibe el código QR, y la identidad se resuelve en el
  centro. Si el cliente no muestra el código, la compra no acumula y no hay forma de
  recuperarla.
- **Los canales excluidos sí pasan por el POS** (el restaurante prepara el pedido), así que el
  motor tiene que filtrarlos por canal (R7).
- **El motor de lealtad es la pieza crítica y la menos documentada.** Guarda cada movimiento
  (ledger), aplica topes y vencimientos, y le devuelve a la app el saldo y los puntos por
  vencer.

---

## 2. Réplica en miniatura — medallion en Databricks

```mermaid
flowchart LR
    GEN["Notebook 00<br/>Generador sintético<br/>reglas R1–R22 + errores E1–E18"]

    subgraph VOL["Volume de Unity Catalog · landing"]
        direction TB
        F1["pos/*.csv"]
        F2["app/*.jsonl"]
        F3["crm/clientes.json"]
        F4["maestros/restaurantes.csv"]
        F5["maestros/catalogo_recompensas.csv"]
        F6["legado/puntos_mcdelivery.csv"]
    end

    subgraph BRZ["BRONZE · tal como llega"]
        direction TB
        B1[("bronze_pos_lineas")]
        B2[("bronze_app_pedidos")]
        B3[("bronze_clientes")]
        B4[("bronze_restaurantes")]
        B5[("bronze_catalogo")]
        B6[("bronze_legado")]
    end

    subgraph SLV["SILVER · limpio y unificado"]
        direction TB
        S1[("silver_clientes")]
        S2[("silver_restaurantes")]
        S3[("silver_transacciones")]
        S4[("silver_lineas")]
        S5[("silver_catalogo<br/>versionado")]
        S6[("silver_legado")]
        SQ[("silver_cuarentena")]
    end

    subgraph GLD["GOLD · reglas de negocio"]
        direction TB
        G0[("gold_lotes_puntos<br/>FIFO applyInPandas")]
        G1[("gold_movimientos_puntos<br/>ledger")]
        G2[("gold_saldos")]
        G3[("gold_puntos_por_vencer")]
        G4[("gold_kpis_mensuales<br/>gold_kpis_canal")]
        G5[("gold_senales_fraude")]
        G6[("gold_violaciones_reglas")]
    end

    subgraph USO["Consumo"]
        direction TB
        U1["05 Evaluación / POC<br/>vs hoja de respuestas"]
        U2["Dashboard"]
        U3["04 Recomendador<br/>MLflow + Unity Catalog"]
        U4[("gold_recomendaciones")]
        U5["App / CRM"]
    end

    GEN --> F1 & F2 & F3 & F4 & F5 & F6
    F1 --> B1
    F2 --> B2
    F3 --> B3
    F4 --> B4
    F5 --> B5
    F6 --> B6

    B1 --> S3 & S4
    B2 --> S3 & S4
    B3 --> S1
    B4 --> S2
    B5 --> S5
    B6 --> S6
    B1 & B2 & B3 & B6 -.-> SQ

    S1 & S3 & S4 & S5 & S6 --> G0
    G0 --> G1
    G0 --> G3
    G1 --> G2 & G5
    G1 & S3 --> G4
    S3 & S4 & S5 & G0 --> G6

    G1 & G6 --> U1
    G2 & G3 & G4 --> U2
    S4 & S5 & G1 --> U3
    U3 --> U4 --> U5

    classDef bronze fill:#e8d2b0,stroke:#8a5a2b,color:#3d2610
    classDef silver fill:#e4e7eb,stroke:#6c757d,color:#212529
    classDef gold fill:#fff1b8,stroke:#b8860b,color:#4d3800
    classDef cuarentena fill:#f8d7da,stroke:#b02a37,color:#58151c
    class B1,B2,B3,B4,B5,B6 bronze
    class S1,S2,S3,S4,S5,S6 silver
    class G0,G1,G2,G3,G4,G5,G6,U4 gold
    class SQ cuarentena
```

**Qué hace cada capa y por qué:**

| Capa | Qué contiene | Decisión | Por qué |
|---|---|---|---|
| **Landing** (Volume) | Los archivos tal como los "envían" los sistemas | Separar archivos de tablas | Se puede reprocesar desde cero y se simula que los datos *llegan*, no que ya están en una tabla |
| **Bronze** | Una tabla por fuente, sin transformar, + `_archivo_origen` y `_fecha_ingesta` | No se limpia nada | Auditoría: si un saldo no cuadra, aquí está el dato original |
| **Silver** | Entidades limpias con un solo formato: horas en `America/Guatemala`, llaves unificadas, POS y app en la **misma** tabla de transacciones | Unificar las dos rutas de entrada | Las reglas de puntos deben aplicarse igual sin importar el canal |
| **Silver · cuarentena** | Registros rechazados + motivo | No borrar en silencio | Un consultor debe poder decir *cuántos* datos se perdieron y *por qué* |
| **Gold** | El **ledger** de puntos y lo que se calcula a partir de él | Las reglas de negocio (R1–R22) viven aquí | Silver describe *qué pasó*; Gold aplica *qué significa* para el programa |

**Relación con la arquitectura actual:** el POS y la app son las fuentes A y B; el motor de
lealtad se convierte en `gold_movimientos_puntos`; el data lake es el medallion completo. El
patrón es el mismo en cualquier nube: en Google Cloud sería Cloud Storage + BigQuery, y aquí
es Volume + tablas Delta.
