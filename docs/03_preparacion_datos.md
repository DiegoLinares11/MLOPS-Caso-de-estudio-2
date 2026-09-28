# Fase 3 — Preparación de datos (CRISP-DM)

**Objetivo de la fase:** construir la réplica medallion en Databricks Free Edition, capa por
capa, documentando cada decisión. La teoría general de las capas está en
[arquitectura_medallion.md](arquitectura_medallion.md); aquí van las decisiones concretas.

| Notebook | Capa | Lee de | Escribe en |
|---|---|---|---|
| [00_generador_datos.py](../notebooks/00_generador_datos.py) | — | — | Volumes `landing` y `control` |
| [01_bronze.py](../notebooks/01_bronze.py) | Bronze | Volume `landing` | 6 tablas `bronze_*` |
| 02_silver.py | Silver | Tablas `bronze_*` | Tablas `silver_*` + `silver_cuarentena` |
| 03_gold.py | Gold | Tablas `silver_*` | Tablas `gold_*` |

Todo vive en `workspace.mimcdonalds`, en el workspace compartido del equipo.

---

## Notebook 00 · Generador

Ejecutado en Databricks (Serverless) con semilla 42. Los conteos de errores coinciden exactamente
con la corrida local, lo que confirma que el generador es **reproducible**.

| Salida | Contenido |
|---|---|
| `landing/pos/` | 390 archivos diarios + 6 reenvíos (E1) |
| `landing/app/` | 390 archivos diarios (fecha UTC) |
| `landing/crm/`, `maestros/`, `legado/` | 4 archivos |
| `control/` | Hoja de respuestas: saldos esperados, errores inyectados, resumen |

## Notebook 01 · Bronze

### Decisiones

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| **Todo como texto** (sin `inferSchema`; `primitivesAsString` en JSON) | Dejar que Spark infiera tipos | Con inferencia, `"22,00"` (E3) se volvería nulo **sin avisar**. Bronze no debe perder ni alterar datos |
| **Una tabla por fuente**, con la granularidad original | Unir POS y app en Bronze | Unificar es transformar; le toca a Silver |
| `items` de la app **sigue anidado** | Aplanarlo con `explode` | Mismo motivo |
| Metadatos `_archivo_origen` (de `_metadata.file_path`) y `_fecha_ingesta` | No guardar el origen | Trazabilidad: cualquier fila se puede rastrear hasta su archivo. `input_file_name()` no funciona con Unity Catalog |
| Partición de carpeta guardada como `_particion_fecha` | Usarla como fecha del ticket | En la app la carpeta es fecha **UTC**; la fecha real del ticket se calcula en Silver |
| `mode("overwrite")` en cada corrida | Anexar (`append`) | Idempotencia: correr dos veces no duplica datos |
| `COMMENT ON TABLE` en cada tabla | Documentar solo en el repo | La descripción queda en Unity Catalog, junto al dato |
| CRM leído con `multiLine=true` | — | El CRM es un solo arreglo JSON, no JSON Lines |

### Resultado esperado (semilla 42)

| Tabla | Filas | Qué contiene |
|---|---|---|
| `bronze_pos_lineas` | 95,055 | Líneas de ticket, incluidas 68 de archivos reenviados y 1,844 filas `ANULADA` |
| `bronze_app_pedidos` | 7,977 | Eventos: 7,589 pedidos + 144 cancelaciones + 244 duplicados |
| `bronze_clientes` | 5,250 | Incluye 150 cuentas duplicadas y 100 de Honduras |
| `bronze_restaurantes` | 30 | |
| `bronze_catalogo` | 12 | Versiones de recompensas |
| `bronze_legado` | 1,817 | Incluye 272 correos no registrados y 7 saldos negativos |

El notebook termina con un `assert` que compara estos números. Si alguno no coincide, se
detiene en lugar de dejar una capa incompleta.

### Cómo se ve en Unity Catalog

En **Catalog → workspace → mimcdonalds → (tabla)**:

- **Overview:** la descripción de `COMMENT ON TABLE` y el esquema (todo `string`).
- **Sample data:** una muestra de filas, con los errores tal como llegaron.
- **Lineage:** el grafo que muestra que la tabla sale del Volume `landing`. Se completará
  solo cuando Silver y Gold lean estas tablas con `spark.table(...)`.
