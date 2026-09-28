# La arquitectura medallion, explicada con un ticket de McDonald's

La arquitectura **medallion** (medallón) es un patrón que popularizó Databricks para organizar
un *lakehouse*. Los datos pasan por **capas de calidad creciente**, y cada capa se construye
**leyendo la tabla de la capa anterior**:

```
Landing  →  Bronze  →  Silver  →  Gold  →  consumo (dashboards, ML, reportes)
archivos    crudo      limpio     negocio
```

## Una analogía: la cocina de un McDonald's

| Capa | En la cocina | En datos |
|---|---|---|
| **Landing** | El camión del proveedor descarga cajas en la puerta | Los archivos llegan a una carpeta |
| **Bronze** | La bodega: las cajas se guardan **tal como llegaron**, con su etiqueta de fecha y proveedor | Tablas con el dato original sin tocar + de dónde y cuándo vino |
| **Silver** | La preparación: lavar, cortar, porcionar, tirar lo que viene en mal estado | Limpiar, tipar, deduplicar, unificar formatos, apartar lo que no sirve |
| **Gold** | La receta: con ingredientes ya preparados se arma el Big Mac que el cliente pide | Aplicar las reglas del negocio para responder preguntas concretas |

Nadie arma una hamburguesa directo desde la caja del proveedor. Y si un cliente se enferma, la
bodega permite rastrear de qué lote vino el ingrediente. Ese es el sentido de guardar Bronze.

---

## Siguiendo un ticket por todas las capas

**La historia:** Ana (cliente `C004821`) pasa por el **AutoMac del restaurante R012** el
3 de octubre de 2025 a las 13:45. Muestra su código QR y pide un Combo Big Mac (Q62.00), un
McFlurry (Q22.00) y dona Q2.00 a la Casa Ronald McDonald.

El POS del restaurante lo registra con **tres problemas típicos**:

- escribe el canal como `AUTO MAC` → error **E2**
- el código de Ana queda en minúsculas y con espacio: ` k7q2m9xa` → error **E4**
- el McFlurry sale con coma decimal: `22,00` → error **E3**

Además, esa noche el restaurante **sube dos veces el archivo** → error **E1**.

### 0. Landing — el archivo tal como llegó

`/Volumes/workspace/mimcdonalds/landing/pos/fecha=2025-10-03/pos_2025-10-03.csv`
(y otra vez en `pos_2025-10-03_reenvio_R012.csv`)

```csv
ticket_id,restaurante_id,fecha_hora_local,canal,codigo_lealtad,linea_num,producto_id,descripcion,cantidad,precio_unitario_q,tipo_linea,recompensa_id,total_ticket_q,estado
R012-20251003-000457,R012,2025-10-03 13:45:10,AUTO MAC, k7q2m9xa,1,P-COMBO-BIGMAC,Combo Big Mac,1,62.00,PRODUCTO,,86.00,COMPLETADA
R012-20251003-000457,R012,2025-10-03 13:45:10,AUTO MAC, k7q2m9xa,2,P-MCFLURRY,McFlurry,1,"22,00",PRODUCTO,,86.00,COMPLETADA
R012-20251003-000457,R012,2025-10-03 13:45:10,AUTO MAC, k7q2m9xa,3,D-RONALD,Donación Casa Ronald McDonald,1,2.00,DONACION,,86.00,COMPLETADA
```

Landing **no es una tabla**, es una carpeta (un *Volume* de Unity Catalog). Separarla de
Bronze permite **reprocesar todo desde cero** si algo sale mal: los archivos siguen ahí.

### 1. Bronze — "tal como llega, pero ya en una tabla"

`workspace.mimcdonalds.bronze_pos_lineas`

| ticket_id | canal | codigo_lealtad | linea_num | precio_unitario_q | … | `_archivo_origen` | `_fecha_ingesta` |
|---|---|---|---|---|---|---|---|
| R012-20251003-000457 | AUTO MAC | ` k7q2m9xa` | 1 | 62.00 | … | pos_2025-10-03.csv | 2026-09-24 10:02 |
| R012-20251003-000457 | AUTO MAC | ` k7q2m9xa` | 2 | 22,00 | … | pos_2025-10-03.csv | 2026-09-24 10:02 |
| R012-20251003-000457 | AUTO MAC | ` k7q2m9xa` | 3 | 2.00 | … | pos_2025-10-03.csv | 2026-09-24 10:02 |
| R012-20251003-000457 | AUTO MAC | ` k7q2m9xa` | 1 | 62.00 | … | pos_2025-10-03_reenvio_R012.csv | 2026-09-24 10:02 |
| … (3 filas más, duplicadas) | | | | | | | |

**Qué se hace:** leer el archivo y guardarlo como tabla Delta. Nada más.

**Qué NO se hace:** no se corrige la coma, no se quitan duplicados, no se cambian tipos.
**Todas las columnas quedan como texto.** Si Bronze intentara convertir `22,00` a número,
fallaría o, peor, lo guardaría como nulo **sin avisar**, y el dato original se perdería.

**Lo único que se agrega** son columnas de **metadatos de ingesta** (`_archivo_origen`,
`_fecha_ingesta`). Son la "etiqueta de la caja": si mañana un saldo no cuadra, se puede
rastrear hasta el archivo exacto.

### 2. Silver — "limpio, tipado y con un solo formato"

Silver lee `bronze_pos_lineas` y `bronze_app_pedidos` y produce **las mismas tablas para los
dos orígenes**:

`silver_transacciones` (una fila por ticket)

| transaccion_id | origen | restaurante_id | customer_id | canal | fecha_hora_gt | fecha_gt | estado | monto_productos_q | monto_donacion_q |
|---|---|---|---|---|---|---|---|---|---|
| R012-20251003-000457 | POS | R012 | **C004821** | **AUTOMAC** | 2025-10-03 13:45:10 | 2025-10-03 | COMPLETADA | **84.00** | 2.00 |

`silver_lineas` (una fila por línea, **sin duplicados**)

| transaccion_id | linea_num | producto_id | cantidad | precio_unitario_q (decimal) | tipo_linea |
|---|---|---|---|---|---|
| R012-20251003-000457 | 1 | P-COMBO-BIGMAC | 1 | 62.00 | PRODUCTO |
| R012-20251003-000457 | 2 | P-MCFLURRY | 1 | **22.00** | PRODUCTO |
| R012-20251003-000457 | 3 | D-RONALD | 1 | 2.00 | DONACION |

**Qué se hizo:**

| Problema | Transformación en Silver |
|---|---|
| E1 archivo duplicado | Deduplicar por `ticket_id + linea_num` |
| E2 `AUTO MAC` | Tabla de mapeo de canales → `AUTOMAC` |
| E3 `22,00` | Reemplazar coma por punto → `decimal(10,2)` |
| E4 ` k7q2m9xa` | `upper(trim())` → `K7Q2M9XA`, y cruce con `silver_clientes` → `C004821` |
| Horas | POS: hora local. App: UTC → se convierte a `America/Guatemala` |

**Qué NO se hace en Silver:** calcular puntos. Silver describe **qué pasó** (Ana compró Q84
en productos y donó Q2), no **qué significa** para el programa. Así, si mañana McDonald's
cambia la tasa a 12 puntos por quetzal, Silver no se toca.

**Lo que no se puede arreglar** (un código QR que no existe, un JSON sin `customer_id`) no se
borra: va a `silver_cuarentena` con el motivo del rechazo.

### 3. Gold — "las reglas del negocio"

Gold lee Silver y aplica las reglas de la Fase 1.

`gold_movimientos_puntos` (el **ledger**)

| customer_id | fecha_gt | tipo | puntos | transaccion_id | vence_el | detalle |
|---|---|---|---|---|---|---|
| C004821 | 2025-10-03 | ACUMULACION | **+700** | R012-20251003-000457 | 2026-10-03 | 840 calculados, recortados por el tope diario |

¿De dónde salen esos números?

1. **R4 y R5:** la base son solo las líneas `PRODUCTO`, así que la donación no cuenta → Q84.00.
2. **R1 y R2:** 10 puntos por quetzal, redondeando hacia arriba → `ceil(84.00 × 10)` = **840**.
3. **R8:** esa mañana Ana ya había ganado 300 puntos en el desayuno, y el tope es de 1,000 por
   día → solo se acreditan **700**. Los 140 restantes se pierden, y eso también se mide.
4. **R10:** el lote vence 365 días después → **2026-10-03**.

A partir del ledger se construyen las demás tablas Gold:

| Tabla | Pregunta que responde |
|---|---|
| `gold_saldos` | ¿Cuántos puntos tiene hoy cada cliente? (suma del ledger) |
| `gold_puntos_por_vencer` | ¿A quién se le vencen puntos en los próximos 30 días? (R11) |
| `gold_kpis_mensuales` / `gold_kpis_canal` | ¿Cuántos puntos se emiten, canjean y vencen por mes? ¿Cómo se comporta cada canal? |
| `gold_violaciones_reglas` | ¿Qué transacciones rompieron una regla del programa? |
| `gold_senales_fraude` | ¿Qué cuentas se comportan raro? (insumo para ML) |

---

## Resumen: responsabilidades de cada capa

| | Landing | Bronze | Silver | Gold |
|---|---|---|---|---|
| **Forma** | Archivos | Tablas Delta | Tablas Delta | Tablas Delta |
| **Tipos** | — | Todo texto | Tipos correctos | Tipos correctos |
| **Duplicados** | Sí | Sí | No | No |
| **Reglas de negocio** | No | No | No | **Sí** |
| **Granularidad** | La de cada fuente | La de cada fuente | Entidades unificadas | Lo que pida el negocio |
| **Quién lo usa** | Solo el pipeline | Ingenieros de datos (auditoría) | Ingenieros y científicos de datos | Analistas, dashboards, modelos |
| **Si algo falla…** | Se vuelven a pedir los archivos | Se reingesta desde Landing | Se reconstruye desde Bronze | Se reconstruye desde Silver |

## Principios que seguimos

1. **Cada capa lee la *tabla* anterior con `spark.table(...)`**, nunca un DataFrame que quedó
   en memoria. Así Unity Catalog registra el **linaje** (el grafo de qué tabla sale de cuál).
2. **Idempotencia:** correr el pipeline dos veces da exactamente el mismo resultado. En la
   réplica cada capa se reescribe completa (`mode("overwrite")`).
3. **Bronze es inmutable:** nunca se corrige un dato ahí.
4. **Nada se pierde en silencio:** lo que no pasa las validaciones va a cuarentena con su motivo.
5. **Las reglas de negocio viven en un solo lugar (Gold).** Si viven repartidas, cuando una
   regla cambia nadie sabe dónde actualizarla. Justamente ese es el problema del cliente.

## Réplica vs. producción

| Aspecto | Nuestra réplica | Cómo sería en producción |
|---|---|---|
| Ingesta | Se leen todos los archivos en cada corrida | **Auto Loader**: solo los archivos nuevos |
| Escritura | `overwrite` de cada tabla | `MERGE` incremental |
| Anulaciones tardías | Llegan en el mismo archivo del día, así que el ticket nunca acumula | Movimiento de `REVERSO` en el ledger cuando la anulación llega después |
| Orquestación | Correr los notebooks en orden | *Lakeflow Jobs* (Workflows) con horario nocturno |
| Calidad | Validaciones en código + cuarentena | *Expectations* de Lakeflow Declarative Pipelines |
