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

## Notebook 02 · Silver

Silver lee **solo tablas** (`spark.table("bronze_...")`), nunca archivos: así Unity Catalog
registra el linaje Bronze → Silver.

### Decisiones

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| `try_cast` en vez de `cast` | `cast` directo | Serverless tiene el modo ANSI activo: un valor inválido con `cast` **detiene todo el pipeline**. Con `try_cast` queda nulo y va a cuarentena |
| Horas como `TIMESTAMP_NTZ` en hora de Guatemala, con `convert_timezone` | `TIMESTAMP` normal con la sesión en UTC | El resultado **no depende de la zona horaria de la sesión**. Se comprobó corriendo Silver con la sesión en UTC y en Asia/Tokio: huellas idénticas |
| POS y app en **la misma** `silver_transacciones` | Una tabla por origen | Las reglas de puntos deben aplicarse igual sin importar el canal |
| Montos recalculados **desde las líneas** | Confiar en `total_ticket_q` | El total del POS puede estar mal (E6); las líneas son la fuente de verdad |
| E5 (código inexistente): el ticket **se conserva sin cliente** | Mandarlo a cuarentena | La venta sí ocurrió; descartarla subestimaría las ventas del restaurante |
| E11 (falta un campo llave en la app): **cuarentena** | Conservarlo con nulos | El contrato de la app promete esos campos; si faltan, el evento está corrupto |
| E13 y E15 (duplicados y Honduras): **se marcan, no se borran** | Excluirlos aquí | Excluir cuentas del programa es una **regla de negocio** (R20, fraude): le toca a Gold |
| Canales: quitar tildes y espacios + **mapa de alias** | Una lista de `if` por variante | Absorbe variantes nuevas (`AUTO-MAC`) sin tocar código; lo desconocido va a cuarentena |
| Departamentos: **tabla de referencia** de los 22 oficiales | Corregir caso por caso | Mismo motivo |
| Teléfonos a **E.164** usando el país de la cuenta | Asumir siempre +502 | Un número de 8 dígitos puede ser de Honduras |
| Cuarentena: **una sola tabla** con fuente, motivo y registro original en JSON | Descartar filas | "¿Cuántos datos perdimos y por qué?" se responde con un `GROUP BY` |
| Expectativas + `assert` al final | Revisar a ojo | Si Silver no cumple, el notebook se detiene y no publica una capa sucia |

### Resultado esperado (semilla 42)

| Tabla | Filas |
|---|---|
| `silver_restaurantes` | 30 |
| `silver_clientes` | 5,250 (300 marcadas como sospecha de duplicado: 150 principales + 150 duplicadas) |
| `silver_catalogo` | 12 |
| `silver_transacciones` | 57,133 (49,607 POS + 7,526 app) |
| `silver_lineas` | 112,870 |
| `silver_legado` | 1,810 (272 pendientes de registro) |
| `silver_cuarentena` | 70 (63 de la app + 7 del legado) |

### Verificación contra la hoja de respuestas

Silver cuenta la huella de cada error y la compara con lo que el generador inyectó. **Los 17
errores que le tocan a Silver cuadran exactamente.** (E10 es una violación de regla y la
detecta Gold.)

Dos conteos difieren del resumen del generador, y está bien que así sea: 5 pedidos con evento
duplicado (E8) y 2 cancelados (E9) **además** tenían un campo faltante (E11). Silver los manda
a cuarentena antes de deduplicar, así que no los cuenta dos veces: 244 → 239 y 144 → 142.

### Cómo se probó antes de subirlo

Spark no puede leer archivos locales en Windows sin los binarios de Hadoop (`winutils`), así que
se armó un arnés de prueba: construye Bronze **en memoria** a partir de los mismos archivos (todo
texto, mismos metadatos) y ejecuta el notebook de Silver celda por celda en Spark 4.2 local, con
el modo ANSI activo como en Serverless.

## Notebook 03 · Gold

Gold es el **único lugar** donde viven las reglas del programa (R1–R22), cada una como una
constante con nombre y su referencia a la Fase 1.

### Decisiones

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| El saldo **se calcula** sumando el ledger | Guardar un saldo que se actualiza | Cualquier saldo se puede auditar hasta la compra que lo originó; es el principio de la contabilidad |
| Tope diario (R8) con una *window*: `min(1000, S) − min(1000, S − puntos)` | Un ciclo por cliente y día | Se resuelve en paralelo con SQL puro, sin Python |
| Vencimiento FIFO con **`applyInPandas`** por cliente | *Window functions* | El vencimiento de cada lote depende de lo que consumieron los canjes y de lo que venció antes: es **secuencial**. `applyInPandas` lo resuelve cliente por cliente, repartiendo los clientes entre los núcleos |
| Costo del canje con *join* por **rango de fechas** contra el catálogo SCD2 | Usar el precio actual | El Big Tasty costaba 7,000 antes de marzo y 7,500 después; usar el precio actual cambiaría saldos históricos |
| `gold_lotes_puntos` se **materializa** como tabla | `.cache()` | Serverless no permite `.cache()`, y el FIFO es el cálculo más caro; además deja evidencia |
| `try_divide` en porcentajes | `/` | En modo ANSI, `0/0` detiene el pipeline (pasa en los canales excluidos, que tienen 0 puntos) |
| Fecha de corte = **último día con datos** | Una fecha fija en el código | El notebook sirve igual si mañana llegan más datos |
| Gold **no lee** la hoja de respuestas | Validar contra `control` aquí | El pipeline no debe depender de la evaluación; Gold se valida solo con un cuadre contable y la comparación va en la Fase 5 |
| Tabla de **violaciones de reglas** | Solo calcular puntos | Gold también audita: una violación es un error del sistema del cliente y un hallazgo para el reporte |

### Resultado (semilla 42)

| Tabla | Filas |
|---|---|
| `gold_lotes_puntos` | 20,062 |
| `gold_movimientos_puntos` | 43,104 |
| `gold_saldos` | 5,150 (todos los clientes de Guatemala, incluidos los que tienen saldo 0) |
| `gold_puntos_por_vencer` | 1,174 lotes |
| `gold_violaciones_reglas` | 34 (todas R15: canjes en McDelivery bajo el mínimo = E10) |
| `gold_kpis_mensuales` | 14 meses |
| `gold_kpis_canal` | 11 canales |
| `gold_senales_fraude` | 5,150 |

**Cuadre contable:** los 6 controles dan 0. El saldo del ledger coincide con lo que queda en
los lotes del FIFO para todos los clientes; no hay saldos negativos, acumulaciones en canales
excluidos o fuera de Guatemala, días por encima del tope ni bienvenidas repetidas.

**Contra la hoja de respuestas** (prueba local, antes de subir): los 5,150 clientes coinciden
en las 7 métricas (acumulados, bienvenida, migrados, canjeados, vencidos, perdidos por tope y
saldo final) con **0 diferencias**. Dos implementaciones independientes de las reglas (Python
puro en el generador, Spark en Gold) llegan al mismo resultado.

### Hallazgos que salen de Gold

| Hallazgo | Dato |
|---|---|
| **El tope diario castiga a McDelivery** | El 60 % de sus pedidos pasa de Q100 y pierde el **35 %** de sus puntos por el tope, contra ~13 % en mostrador, AutoMac y kiosco |
| El programa emite más puntos por **migración** que por compras | 19.5 M migrados vs 18.3 M acumulados en 13 meses |
| **Breakage** alto | 9.3 M puntos vencidos sin usarse, la mitad de lo acumulado por compras |
| La app no valida el mínimo de canje en McDelivery | 34 canjes bajo Q50/Q60 (R15) |
| Los canales de terceros mueven ventas grandes que no se miden | PedidosYa y Uber Eats: ticket promedio de ~Q129, 0 puntos y 0 clientes identificados |
