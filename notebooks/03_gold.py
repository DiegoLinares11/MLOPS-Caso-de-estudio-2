# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa GOLD — las reglas del programa
# MAGIC
# MAGIC Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala
# MAGIC
# MAGIC Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis
# MAGIC
# MAGIC Silver dejó **qué pasó** (compras, canjes, clientes). Gold decide **qué significa** para
# MAGIC el programa: aquí, y solo aquí, viven las reglas R1–R22 de la Fase 1.
# MAGIC
# MAGIC | Tabla | Pregunta que responde |
# MAGIC |---|---|
# MAGIC | `gold_movimientos_puntos` | El **ledger**: cada punto que entró o salió, con su motivo |
# MAGIC | `gold_lotes_puntos` | Resultado del FIFO: vencimientos y lo que queda de cada lote al corte |
# MAGIC | `gold_saldos` | ¿Cuántos puntos tiene cada cliente al cierre? |
# MAGIC | `gold_puntos_por_vencer` | ¿A quién se le vencen puntos en los próximos 30 días? (R11) |
# MAGIC | `gold_kpis_mensuales` | ¿Cómo evoluciona el programa mes a mes? |
# MAGIC | `gold_kpis_canal` | ¿Cómo se comporta cada canal? |
# MAGIC | `gold_violaciones_reglas` | ¿Qué transacciones rompieron una regla del programa? |
# MAGIC | `gold_senales_fraude` | Variables por cliente para detectar abuso (insumo de la Fase 4) |
# MAGIC
# MAGIC **El saldo nunca se guarda: se calcula** sumando el ledger. Es el mismo principio de la
# MAGIC contabilidad, y permite auditar cualquier saldo hasta la compra que lo originó.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración y reglas del programa
# MAGIC
# MAGIC Las reglas están **con nombre y en un solo lugar**, cada una con su referencia a la Fase 1.

# COMMAND ----------

from datetime import date, timedelta

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOGO = "workspace"
ESQUEMA = "mimcdonalds"
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

PAIS_PROGRAMA = "GT"                                                   # R20
PUNTOS_POR_QUETZAL = 10                                                # R1 (R2: redondeo hacia arriba)
CANALES_EXCLUIDOS = ["PEDIDOSYA", "UBEREATS", "CALLCENTER", "WHATSAPP"]  # R7
TOPE_DIARIO_ACUMULACION = 1_000                                        # R8
DIAS_VIGENCIA = 365                                                    # R10
DIAS_AVISO_VENCIMIENTO = 30                                            # R11
TOPE_DIARIO_CANJE = 15_000                                             # R12
MAX_OFERTAS_POR_PEDIDO = 3                                             # R13
MINIMO_CANJE_DELIVERY_Q = {"DESAYUNO": 50, "ALMUERZO_CENA": 60}        # R15
PUNTOS_BIENVENIDA = 1_000                                              # R17
FACTOR_MIGRACION = 10                                                  # R21 / H1 (supuesto)
FECHA_LANZAMIENTO = date(2025, 8, 27)

transacciones = spark.table("silver_transacciones")
FECHA_CORTE = transacciones.agg(F.max("fecha_gt")).first()[0]
print(f"[gold] fecha de corte (último día con datos): {FECHA_CORTE}")

clientes_gt = (spark.table("silver_clientes")
                    .filter(F.col("pais") == PAIS_PROGRAMA)
                    .select("customer_id", "fecha_registro_gt"))


def escribir_gold(df, tabla, descripcion):
    nombre = f"{CATALOGO}.{ESQUEMA}.{tabla}"
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre))
    spark.sql(f"COMMENT ON TABLE {nombre} IS '{descripcion}'")
    filas = spark.table(nombre).count()
    print(f"[gold] {tabla:<26} {filas:>8,} filas")
    return filas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Acumulación por compras · R1, R2, R4, R7, R8, R20
# MAGIC
# MAGIC Una compra acumula si: el cliente es de Guatemala (R20), el canal participa (R7), no se
# MAGIC anuló (H3) y compró productos. La base son **solo los productos**: las donaciones (R4) ya
# MAGIC vienen separadas desde Silver.
# MAGIC
# MAGIC **El tope diario (R8) sin ciclos.** Se ordenan las compras del cliente en el día y se toma la
# MAGIC suma acumulada `S`. Lo otorgado hasta esa compra es `min(1000, S)`, así que cada compra recibe
# MAGIC `min(1000, S) − min(1000, S − puntos)`.
# MAGIC
# MAGIC | Compra | Puntos | `S` | `min(1000,S)` | Otorgados | Perdidos |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Desayuno 8:00 | 300 | 300 | 300 | 300 | 0 |
# MAGIC | Almuerzo 13:45 | 840 | 1,140 | 1,000 | **700** | 140 |
# MAGIC | Cena 20:00 | 450 | 1,590 | 1,000 | **0** | 450 |

# COMMAND ----------

compras = (
    transacciones
    .join(clientes_gt.select("customer_id"), "customer_id")                    # R20
    .filter(~F.col("canal").isin(CANALES_EXCLUIDOS))                           # R7
    .filter(F.col("estado") == "COMPLETADA")                                   # H3
    .filter(F.col("monto_productos_q") > 0)
    .withColumn("puntos_calculados",                                           # R1 + R2
                F.ceil(F.col("monto_productos_q") * PUNTOS_POR_QUETZAL).cast("int"))
)

mismo_dia = (Window.partitionBy("customer_id", "fecha_gt")
                   .orderBy("fecha_hora_gt", "transaccion_id")
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))
tope = F.lit(TOPE_DIARIO_ACUMULACION)

compras = (
    compras
    .withColumn("suma_dia", F.sum("puntos_calculados").over(mismo_dia))
    .withColumn("puntos_otorgados",                                            # R8
                F.least(tope, F.col("suma_dia"))
                - F.least(tope, F.col("suma_dia") - F.col("puntos_calculados")))
    .withColumn("puntos_perdidos_por_tope", F.col("puntos_calculados") - F.col("puntos_otorgados"))
)

acumulaciones = compras.filter(F.col("puntos_otorgados") > 0).select(
    "customer_id", "fecha_gt", "fecha_hora_gt", F.lit(2).alias("prioridad"),
    F.lit("ACUMULACION").alias("tipo"), F.col("puntos_otorgados").alias("puntos"),
    "transaccion_id", F.lit(None).cast("string").alias("recompensa_id"),
    "puntos_calculados", "puntos_perdidos_por_tope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bienvenida (R17) y migración del programa anterior (R21)
# MAGIC
# MAGIC - **Bienvenida:** 1,000 puntos con la **primera compra que acumula**. No cuenta para el tope.
# MAGIC - **Migración:** saldo anterior × 10 (supuesto H1: se conserva el valor en quetzales), abonado el
# MAGIC   día del registro en MiMcDonald's o el del lanzamiento, lo que ocurra después.

# COMMAND ----------

primera_compra = Window.partitionBy("customer_id").orderBy("fecha_hora_gt", "transaccion_id")
bienvenidas = (
    compras.withColumn("n", F.row_number().over(primera_compra)).filter("n = 1")
           .select("customer_id", "fecha_gt", "fecha_hora_gt", F.lit(3).alias("prioridad"),
                   F.lit("BIENVENIDA").alias("tipo"), F.lit(PUNTOS_BIENVENIDA).alias("puntos"),
                   "transaccion_id", F.lit(None).cast("string").alias("recompensa_id"),
                   F.lit(None).cast("int").alias("puntos_calculados"),
                   F.lit(None).cast("int").alias("puntos_perdidos_por_tope"))
)

fecha_migracion = F.greatest(F.lit(FECHA_LANZAMIENTO), F.col("fecha_registro_gt"))
migraciones = (
    spark.table("silver_legado")
         .filter((F.col("estado_migracion") == "REGISTRADO") & (F.col("saldo_puntos_legado") > 0))
         .join(clientes_gt, "customer_id")
         .select("customer_id", fecha_migracion.alias("fecha_gt"),
                 fecha_migracion.cast("timestamp_ntz").alias("fecha_hora_gt"), F.lit(0).alias("prioridad"),
                 F.lit("MIGRACION").alias("tipo"),
                 (F.col("saldo_puntos_legado") * FACTOR_MIGRACION).alias("puntos"),
                 F.lit(None).cast("string").alias("transaccion_id"),
                 F.lit(None).cast("string").alias("recompensa_id"),
                 F.lit(None).cast("int").alias("puntos_calculados"),
                 F.lit(None).cast("int").alias("puntos_perdidos_por_tope"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Canjes · R14, R16
# MAGIC
# MAGIC - El costo sale de la versión del catálogo **vigente el día del canje** (R16). Es un *join* por
# MAGIC   rango de fechas contra la dimensión SCD2: el Big Tasty cuesta 7,000 antes de marzo y 7,500
# MAGIC   después.
# MAGIC - Un canje **cuenta aunque la orden se anule** (R14).
# MAGIC - La quesoburguesa de bienvenida cuesta 0 puntos: no entra al ledger, pero sí a las
# MAGIC   validaciones y a las señales de fraude.

# COMMAND ----------

catalogo = spark.table("silver_catalogo").select(
    F.col("recompensa_id").alias("rid"), F.col("puntos").alias("costo"), "vigente_desde", "vigente_hasta")

canjes = (
    spark.table("silver_lineas").filter(F.col("tipo_linea") == "CANJE")
         .select("transaccion_id", "linea_num", "recompensa_id")
         .join(transacciones.select("transaccion_id", "customer_id", "fecha_gt", "fecha_hora_gt",
                                    "canal", "franja", "monto_productos_q"), "transaccion_id")
         .join(clientes_gt.select("customer_id"), "customer_id")                               # R20
         .join(catalogo,
               (F.col("recompensa_id") == F.col("rid"))
               & (F.col("fecha_gt") >= F.col("vigente_desde"))
               & (F.col("fecha_gt") <= F.coalesce(F.col("vigente_hasta"), F.lit(date(9999, 12, 31)))),
               "left")                                                                         # R16
)

movimientos_canje = canjes.filter(F.col("costo") > 0).select(
    "customer_id", "fecha_gt", "fecha_hora_gt", F.lit(1).alias("prioridad"),
    F.lit("CANJE").alias("tipo"), (-F.col("costo")).alias("puntos"),
    "transaccion_id", "recompensa_id",
    F.lit(None).cast("int").alias("puntos_calculados"),
    F.lit(None).cast("int").alias("puntos_perdidos_por_tope"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vencimientos por lote (R10) con `applyInPandas`
# MAGIC
# MAGIC Cada abono es un **lote** que vence 365 días después. Cada canje consume **primero los lotes
# MAGIC más viejos** (FIFO). Cuánto vence un lote depende de lo que consumieron los canjes anteriores,
# MAGIC y eso depende de lo que venció antes: es **secuencial por naturaleza** y no cabe en una
# MAGIC *window function*.
# MAGIC
# MAGIC `applyInPandas` resuelve eso: Spark agrupa por cliente y a cada grupo le aplica una función de
# MAGIC Python que recorre sus movimientos en orden. Los ~5,000 clientes se reparten entre los núcleos,
# MAGIC así que sigue siendo **paralelo**.
# MAGIC
# MAGIC La función devuelve tres cosas:
# MAGIC - `VENCIMIENTO`: lo que venció de cada lote, en la fecha en que venció.
# MAGIC - `LOTE_VIGENTE`: lo que queda de cada lote al corte (para `gold_puntos_por_vencer`).
# MAGIC - `CANJE_SIN_SALDO`: un canje que usó más puntos de los disponibles (no debería pasar).
# MAGIC
# MAGIC Supuestos (los mismos de la Fase 2): el vencimiento ocurre **al inicio del día**, y un canje
# MAGIC solo puede usar lotes de **días anteriores** porque la acreditación tarda hasta 24 h (R9).

# COMMAND ----------

movimientos = (acumulaciones.unionByName(bienvenidas)
                            .unionByName(migraciones)
                            .unionByName(movimientos_canje))

ESQUEMA_LOTES = "customer_id string, tipo string, fecha_gt date, puntos long, fecha_lote date, vence_el date"
VIGENCIA = timedelta(days=DIAS_VIGENCIA)


def procesar_lotes(pdf: pd.DataFrame) -> pd.DataFrame:
    cliente = pdf["customer_id"].iloc[0]
    pdf = pdf.assign(fecha_gt=pd.to_datetime(pdf["fecha_gt"]).dt.date)
    pdf = pdf.sort_values(["fecha_gt", "fecha_hora_gt", "prioridad"], kind="stable")
    lotes, salida = [], []           # lote = [fecha, puntos_restantes]

    def vencer(hasta):
        for lote in lotes:
            if lote[1] > 0 and lote[0] + VIGENCIA <= hasta:
                salida.append((cliente, "VENCIMIENTO", lote[0] + VIGENCIA, -lote[1], lote[0], lote[0] + VIGENCIA))
                lote[1] = 0

    for fila in pdf.itertuples(index=False):
        vencer(fila.fecha_gt)
        if fila.puntos > 0:                                  # abono: nace un lote
            lotes.append([fila.fecha_gt, int(fila.puntos)])
            continue
        costo = -int(fila.puntos)                            # canje: consume FIFO
        disponible = sum(r for f, r in lotes if f < fila.fecha_gt)
        if disponible < costo:
            salida.append((cliente, "CANJE_SIN_SALDO", fila.fecha_gt, costo - disponible, None, None))
        for lote in lotes:
            usar = min(lote[1], costo)
            lote[1] -= usar
            costo -= usar

    vencer(FECHA_CORTE)
    salida += [(cliente, "LOTE_VIGENTE", f, r, f, f + VIGENCIA) for f, r in lotes if r > 0]
    return pd.DataFrame(salida, columns=["customer_id", "tipo", "fecha_gt", "puntos", "fecha_lote", "vence_el"])


lotes = (movimientos.select("customer_id", "fecha_gt", "fecha_hora_gt", "prioridad", "puntos")
                    .groupBy("customer_id")
                    .applyInPandas(procesar_lotes, schema=ESQUEMA_LOTES))

# Se materializa en una tabla: el FIFO es el cálculo más caro y se usa varias veces más abajo.
# (En Serverless no existe .cache(); guardar la tabla cumple la misma función y deja evidencia.)
escribir_gold(lotes, "gold_lotes_puntos",
              "Resultado del FIFO por cliente: vencimientos, lotes vigentes al corte y canjes sin saldo.")
lotes = spark.table("gold_lotes_puntos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. El ledger: `gold_movimientos_puntos`
# MAGIC
# MAGIC Todos los movimientos en una sola tabla, con signo: los abonos suman y los canjes y
# MAGIC vencimientos restan. Cada abono lleva su fecha de vencimiento (`vence_el`).

# COMMAND ----------

vencimientos = lotes.filter(F.col("tipo") == "VENCIMIENTO").select(
    "customer_id", "fecha_gt", F.col("fecha_gt").cast("timestamp_ntz").alias("fecha_hora_gt"),
    F.lit(-1).alias("prioridad"), "tipo", "puntos",
    F.lit(None).cast("string").alias("transaccion_id"), F.lit(None).cast("string").alias("recompensa_id"),
    F.lit(None).cast("int").alias("puntos_calculados"), F.lit(None).cast("int").alias("puntos_perdidos_por_tope"))

ledger = (
    movimientos.unionByName(vencimientos)
               .withColumn("puntos", F.col("puntos").cast("long"))
               .withColumn("vence_el", F.when(F.col("puntos") > 0, F.date_add("fecha_gt", DIAS_VIGENCIA)))
               .drop("prioridad")
)

escribir_gold(ledger, "gold_movimientos_puntos",
              "Ledger de puntos: acumulacion, bienvenida, migracion, canje y vencimiento, con signo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Saldos y puntos por vencer · R11
# MAGIC
# MAGIC El saldo de cada cliente es la **suma de su ledger**. Se incluyen los clientes sin
# MAGIC movimientos (saldo 0), porque también son parte del programa.

# COMMAND ----------

mov = spark.table("gold_movimientos_puntos")


def suma_tipo(tipo, signo=1):
    return (signo * F.sum(F.when(F.col("tipo") == tipo, F.col("puntos")).otherwise(0))).cast("long")


por_cliente = mov.groupBy("customer_id").agg(
    suma_tipo("ACUMULACION").alias("puntos_acumulados"),
    suma_tipo("BIENVENIDA").alias("puntos_bienvenida"),
    suma_tipo("MIGRACION").alias("puntos_migrados"),
    suma_tipo("CANJE", -1).alias("puntos_canjeados"),
    suma_tipo("VENCIMIENTO", -1).alias("puntos_vencidos"),
    F.sum("puntos").cast("long").alias("saldo_final"),
)
perdidos = compras.groupBy("customer_id").agg(
    F.sum("puntos_perdidos_por_tope").cast("long").alias("puntos_perdidos_por_tope"),
    F.max("fecha_gt").alias("ultima_compra"))

limite_aviso = F.date_add(F.lit(FECHA_CORTE), DIAS_AVISO_VENCIMIENTO)
por_vencer = (lotes.filter((F.col("tipo") == "LOTE_VIGENTE") & (F.col("vence_el") <= limite_aviso))
                   .groupBy("customer_id")
                   .agg(F.sum("puntos").cast("long").alias("puntos_por_vencer_30d"),
                        F.min("vence_el").alias("proximo_vencimiento")))

saldos = (clientes_gt.select("customer_id")
                     .join(por_cliente, "customer_id", "left")
                     .join(perdidos, "customer_id", "left")
                     .join(por_vencer, "customer_id", "left")
                     .fillna(0, subset=["puntos_acumulados", "puntos_bienvenida", "puntos_migrados",
                                        "puntos_canjeados", "puntos_vencidos", "saldo_final",
                                        "puntos_perdidos_por_tope", "puntos_por_vencer_30d"])
                     .withColumn("fecha_corte", F.lit(FECHA_CORTE)))

escribir_gold(saldos, "gold_saldos",
              "Saldo de puntos por cliente al corte, desglosado por tipo de movimiento.")

escribir_gold(
    lotes.filter((F.col("tipo") == "LOTE_VIGENTE") & (F.col("vence_el") <= limite_aviso))
         .select("customer_id", F.col("fecha_lote"), "vence_el", F.col("puntos").alias("puntos_por_vencer"))
         .withColumn("dias_para_vencer", F.datediff("vence_el", F.lit(FECHA_CORTE))),
    "gold_puntos_por_vencer",
    "Lotes que vencen en los proximos 30 dias: base para el aviso al cliente (R11).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Violaciones de reglas · R12, R13, R15 y consistencia
# MAGIC
# MAGIC Gold no solo calcula: también **audita** si las transacciones respetaron las reglas. Una
# MAGIC violación indica un error en el sistema del cliente (por ejemplo, la app dejó canjear en
# MAGIC McDelivery sin llegar al mínimo) y es un hallazgo para el reporte.

# COMMAND ----------

minimo = F.when(F.col("franja") == "DESAYUNO", MINIMO_CANJE_DELIVERY_Q["DESAYUNO"]) \
          .otherwise(MINIMO_CANJE_DELIVERY_Q["ALMUERZO_CENA"])

r15 = (canjes.filter((F.col("canal") == "MCDELIVERY") & (F.col("monto_productos_q") < minimo))
             .groupBy("transaccion_id", "customer_id", "fecha_gt")
             .agg(F.first("monto_productos_q").alias("monto"), F.first("franja").alias("franja"))
             .select(F.lit("R15 canje en McDelivery bajo el minimo").alias("regla"), "transaccion_id",
                     "customer_id", "fecha_gt",
                     F.concat(F.lit("compra Q"), F.col("monto").cast("string"), F.lit(" en "), F.col("franja")).alias("detalle")))

r13 = (canjes.groupBy("transaccion_id", "customer_id", "fecha_gt").count()
             .filter(F.col("count") > MAX_OFERTAS_POR_PEDIDO)
             .select(F.lit("R13 mas de 3 ofertas por pedido").alias("regla"), "transaccion_id",
                     "customer_id", "fecha_gt", F.concat(F.col("count").cast("string"), F.lit(" ofertas")).alias("detalle")))

r12 = (canjes.groupBy("customer_id", "fecha_gt").agg(F.sum("costo").alias("pts"))
             .filter(F.col("pts") > TOPE_DIARIO_CANJE)
             .select(F.lit("R12 tope diario de canje").alias("regla"), F.lit(None).cast("string").alias("transaccion_id"),
                     "customer_id", "fecha_gt", F.concat(F.col("pts").cast("string"), F.lit(" puntos")).alias("detalle")))

sin_catalogo = (canjes.filter(F.col("costo").isNull())
                      .select(F.lit("R16 recompensa fuera del catalogo vigente").alias("regla"), "transaccion_id",
                              "customer_id", "fecha_gt", F.col("recompensa_id").alias("detalle")))

bienvenida_repetida = (canjes.filter(F.col("recompensa_id") == "RW-BIENVENIDA")
                             .groupBy("customer_id").agg(F.count("*").alias("n"), F.min("fecha_gt").alias("fecha_gt"))
                             .filter("n > 1")
                             .select(F.lit("R17 bienvenida canjeada mas de una vez").alias("regla"),
                                     F.lit(None).cast("string").alias("transaccion_id"), "customer_id", "fecha_gt",
                                     F.concat(F.col("n").cast("string"), F.lit(" veces")).alias("detalle")))

sin_saldo = (lotes.filter(F.col("tipo") == "CANJE_SIN_SALDO")
                  .select(F.lit("Canje sin saldo suficiente").alias("regla"), F.lit(None).cast("string").alias("transaccion_id"),
                          "customer_id", "fecha_gt", F.concat(F.lit("faltaron "), F.col("puntos").cast("string")).alias("detalle")))

violaciones = r15.unionByName(r13).unionByName(r12).unionByName(sin_catalogo) \
                 .unionByName(bienvenida_repetida).unionByName(sin_saldo)

escribir_gold(violaciones, "gold_violaciones_reglas",
              "Transacciones y clientes que rompieron una regla del programa.")
display(spark.table("gold_violaciones_reglas").groupBy("regla").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. KPIs del programa
# MAGIC
# MAGIC - **Tasa de identificación:** de las compras en canales que participan, ¿qué porcentaje se
# MAGIC   hizo mostrando el QR? Cada compra sin QR es un cliente que **no se está midiendo**.
# MAGIC - **Breakage:** puntos que vencen sin usarse. Para la empresa es un pasivo que desaparece;
# MAGIC   para el cliente, una mala experiencia.

# COMMAND ----------

completadas = transacciones.filter(F.col("estado") == "COMPLETADA")
participan = completadas.filter(~F.col("canal").isin(CANALES_EXCLUIDOS))
mes = F.date_trunc("month", F.col("fecha_gt")).cast("date")

ventas_mes = completadas.groupBy(mes.alias("mes")).agg(
    F.count("*").alias("tickets"),
    F.sum("monto_productos_q").alias("ventas_q"))
identificacion_mes = participan.groupBy(mes.alias("mes")).agg(
    F.count("*").alias("tickets_canales_participantes"))
compras_mes = compras.groupBy(mes.alias("mes")).agg(
    F.count("*").alias("tickets_identificados"),
    F.countDistinct("customer_id").alias("clientes_activos"),
    F.sum("puntos_perdidos_por_tope").alias("puntos_perdidos_por_tope"))
puntos_mes = mov.groupBy(mes.alias("mes")).pivot(
    "tipo", ["ACUMULACION", "BIENVENIDA", "MIGRACION", "CANJE", "VENCIMIENTO"]).agg(F.sum("puntos"))

kpis_mes = (ventas_mes.join(identificacion_mes, "mes", "left").join(compras_mes, "mes", "left")
                      .join(puntos_mes, "mes", "left").fillna(0)
                      .withColumn("tasa_identificacion",
                                  F.round(F.try_divide(F.col("tickets_identificados"),
                                                       F.col("tickets_canales_participantes")), 4))
                      .withColumnRenamed("ACUMULACION", "puntos_acumulados")
                      .withColumnRenamed("BIENVENIDA", "puntos_bienvenida")
                      .withColumnRenamed("MIGRACION", "puntos_migrados")
                      .withColumn("puntos_canjeados", -F.col("CANJE")).drop("CANJE")
                      .withColumn("puntos_vencidos", -F.col("VENCIMIENTO")).drop("VENCIMIENTO")
                      .orderBy("mes"))

escribir_gold(kpis_mes, "gold_kpis_mensuales",
              "KPIs mensuales: ventas, identificacion, clientes activos y puntos por tipo.")

kpis_canal = (
    completadas.groupBy("canal").agg(
        F.count("*").alias("tickets"),
        F.round(F.avg("monto_productos_q"), 2).alias("ticket_promedio_q"),
        F.round(F.avg((F.col("monto_productos_q") > 100).cast("int")), 4).alias("pct_tickets_mayores_q100"))
    .join(compras.groupBy("canal").agg(
        F.count("*").alias("tickets_identificados"),
        F.sum("puntos_otorgados").alias("puntos_otorgados"),
        F.sum("puntos_perdidos_por_tope").alias("puntos_perdidos_por_tope")), "canal", "left")
    .fillna(0)
    .withColumn("participa_en_programa", ~F.col("canal").isin(CANALES_EXCLUIDOS))
    .withColumn("pct_puntos_perdidos_por_tope",                 # try_divide: 0/0 -> nulo, no error ANSI
                F.round(F.try_divide(F.col("puntos_perdidos_por_tope"),
                                     F.col("puntos_otorgados") + F.col("puntos_perdidos_por_tope")), 4))
    .orderBy(F.desc("tickets"))
)

escribir_gold(kpis_canal, "gold_kpis_canal",
              "KPIs por canal: ticket promedio, identificacion y puntos perdidos por el tope diario.")
display(spark.table("gold_kpis_canal"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Señales de fraude (insumo para la Fase 4)
# MAGIC
# MAGIC Una fila por cliente con variables que describen su comportamiento. **No es un modelo**: es la
# MAGIC tabla de *features* sobre la que se entrenaría uno. El patrón de H6 (varias cuentas de la misma
# MAGIC persona para cobrar la bienvenida) debería notarse en `es_cuenta_secundaria`,
# MAGIC `dias_registro_a_primera_compra` y `compras`.

# COMMAND ----------

bienvenida_canjeada = (canjes.filter(F.col("recompensa_id") == "RW-BIENVENIDA")
                             .groupBy("customer_id").agg(F.lit(True).alias("canjeo_bienvenida")))
actividad = compras.groupBy("customer_id").agg(
    F.count("*").alias("compras"),
    F.sum("monto_productos_q").alias("gasto_total_q"),
    F.min("fecha_gt").alias("primera_compra"),
    F.max("fecha_gt").alias("ultima_compra"))
canjes_cliente = canjes.filter(F.col("costo") > 0).groupBy("customer_id").agg(F.count("*").alias("canjes"))

senales = (
    spark.table("silver_clientes").filter(F.col("pais") == PAIS_PROGRAMA)
         .select("customer_id", "fecha_registro_gt", "n_cuentas_misma_persona", "sospecha_duplicado",
                 (F.col("sospecha_duplicado") & (F.col("customer_id") != F.col("cuenta_principal_id"))).alias("es_cuenta_secundaria"))
         .join(actividad, "customer_id", "left")
         .join(canjes_cliente, "customer_id", "left")
         .join(bienvenida_canjeada, "customer_id", "left")
         .join(spark.table("gold_saldos").select("customer_id", "saldo_final", "puntos_vencidos"), "customer_id", "left")
         .fillna({"compras": 0, "canjes": 0, "canjeo_bienvenida": False})
         .withColumn("dias_registro_a_primera_compra", F.datediff("primera_compra", "fecha_registro_gt"))
         .withColumn("dias_activo", F.datediff("ultima_compra", "primera_compra"))
         .withColumn("dias_desde_ultima_compra", F.datediff(F.lit(FECHA_CORTE), "ultima_compra"))
)

escribir_gold(senales, "gold_senales_fraude",
              "Variables por cliente para detectar abuso del programa y abandono (features para ML).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Comprobaciones
# MAGIC
# MAGIC ### 11.1 Cuadre contable
# MAGIC
# MAGIC El saldo se calcula de **dos formas independientes**: sumando el ledger y sumando lo que queda
# MAGIC en los lotes del FIFO. Si las dos no coinciden para **todos** los clientes, hay un error en
# MAGIC Gold. Además, ninguna regla puede haberse aplicado mal.

# COMMAND ----------

s = spark.table("gold_saldos")
restante_lotes = lotes.filter(F.col("tipo") == "LOTE_VIGENTE").groupBy("customer_id").agg(F.sum("puntos").alias("en_lotes"))

cuadre = {
    "clientes cuyo saldo no cuadra con sus lotes":
        s.join(restante_lotes, "customer_id", "left").fillna(0, ["en_lotes"])
         .filter(F.col("saldo_final") != F.col("en_lotes")).count(),
    "clientes con saldo negativo": s.filter(F.col("saldo_final") < 0).count(),
    "acumulaciones en canales excluidos (R7)":
        mov.filter(F.col("tipo") == "ACUMULACION").join(transacciones.select("transaccion_id", "canal"), "transaccion_id")
           .filter(F.col("canal").isin(CANALES_EXCLUIDOS)).count(),
    "acumulaciones de cuentas fuera de Guatemala (R20)":
        mov.join(clientes_gt, "customer_id", "left_anti").count(),
    "clientes-dia por encima del tope de 1,000 (R8)":
        mov.filter(F.col("tipo") == "ACUMULACION").groupBy("customer_id", "fecha_gt")
           .agg(F.sum("puntos").alias("p")).filter(F.col("p") > TOPE_DIARIO_ACUMULACION).count(),
    "clientes con mas de una bienvenida (R17)":
        mov.filter(F.col("tipo") == "BIENVENIDA").groupBy("customer_id").count().filter("count > 1").count(),
}
for nombre, valor in cuadre.items():
    print(f"{'OK ' if valor == 0 else 'FALLA'} {nombre:<50} {valor}")
assert all(v == 0 for v in cuadre.values()), "Gold no cuadra"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Totales del programa
# MAGIC
# MAGIC La comparación **cliente por cliente** contra la hoja de respuestas del generador es el POC de
# MAGIC la Fase 5 (`05_evaluacion`). Aquí se muestran los totales como primera señal.

# COMMAND ----------

totales = s.agg(*[F.sum(c).alias(c) for c in
                  ["puntos_acumulados", "puntos_bienvenida", "puntos_migrados", "puntos_canjeados",
                   "puntos_vencidos", "puntos_perdidos_por_tope", "saldo_final"]]).first().asDict()
for nombre, valor in totales.items():
    print(f"{nombre:<26} {valor:>14,}")

display(spark.table("gold_violaciones_reglas").groupBy("regla").count())
