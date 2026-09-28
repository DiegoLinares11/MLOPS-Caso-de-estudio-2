# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Evaluación y prueba funcional (POC) del sistema de puntos
# MAGIC
# MAGIC Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala
# MAGIC
# MAGIC Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis
# MAGIC
# MAGIC **Fase 5 de CRISP-DM:** ¿lo que construimos cumple lo que el negocio necesita?
# MAGIC
# MAGIC | Parte | Pregunta |
# MAGIC |---|---|
# MAGIC | **A. Pruebas funcionales** | ¿Cada regla del programa (R1–R21) se aplica correctamente? |
# MAGIC | **B. Calidad de datos** | ¿Silver detectó **exactamente** los errores inyectados, ni más ni menos? |
# MAGIC | **C. Saldos** | ¿Gold calcula los mismos puntos que la hoja de respuestas, cliente por cliente? |
# MAGIC | **D. Recomendador** | ¿El modelo aprendió los gustos reales de los clientes? |
# MAGIC
# MAGIC Es el **único notebook que lee el Volume `control`** (la hoja de respuestas del generador).
# MAGIC El pipeline (01–04) nunca la lee: en producción no existe. Aquí se usa para **evaluar** el
# MAGIC pipeline desde afuera.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

import json

import pandas as pd
from pyspark.sql import functions as F

CATALOGO = "workspace"
ESQUEMA = "mimcdonalds"
CONTROL = f"/Volumes/{CATALOGO}/{ESQUEMA}/control"
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

resultados = []          # (parte, prueba, esperado, obtenido, ok)


def escribir_tabla(df, tabla, descripcion):
    nombre = f"{CATALOGO}.{ESQUEMA}.{tabla}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre)
    spark.sql(f"COMMENT ON TABLE {nombre} IS '{descripcion}'")
    return spark.table(nombre).count()


def registrar(parte, prueba, esperado, obtenido, ok=None):
    ok = (esperado == obtenido) if ok is None else ok
    resultados.append((parte, prueba, str(esperado), str(obtenido), bool(ok)))
    print(f"{'OK   ' if ok else 'FALLA'} [{parte}] {prueba:<62} esperado {esperado} · obtenido {obtenido}")


t = spark.table("silver_transacciones")
lineas = spark.table("silver_lineas")
clientes = spark.table("silver_clientes")
mov = spark.table("gold_movimientos_puntos")
lotes = spark.table("gold_lotes_puntos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A · Pruebas funcionales por regla
# MAGIC
# MAGIC Cada prueba toma **todos** los casos de los datos donde aplica la regla, calcula el resultado
# MAGIC esperado de forma independiente y cuenta las fallas (deben ser 0). Además muestra **un
# MAGIC ejemplo concreto**, para que la prueba se pueda leer en el reporte.

# COMMAND ----------

# MAGIC %md
# MAGIC ### R1 + R2 + R4 · 10 puntos por quetzal, redondeo hacia arriba, sin donaciones
# MAGIC
# MAGIC Esperado: `ceil(monto de productos × 10)`, calculado desde las líneas de Silver, sin contar
# MAGIC donaciones.

# COMMAND ----------

acum = (mov.filter(F.col("tipo") == "ACUMULACION")
           .join(t.select("transaccion_id", "monto_productos_q", "monto_donacion_q"), "transaccion_id"))
esperado_r1 = F.ceil(F.col("monto_productos_q") * 10)
fallas = acum.filter(F.col("puntos_calculados") != esperado_r1).count()
registrar("A", "R1+R2 puntos = ceil(monto productos x 10)", 0, fallas)

ej = (acum.filter((F.col("monto_productos_q") * 10) != F.floor(F.col("monto_productos_q") * 10))
          .select("transaccion_id", "monto_productos_q", "puntos_calculados").first())
print(f"      ejemplo de redondeo: {ej.transaccion_id} · Q{ej.monto_productos_q} x 10 = "
      f"{float(ej.monto_productos_q) * 10} -> {ej.puntos_calculados} puntos")

con_donacion = acum.filter(F.col("monto_donacion_q") > 0)
fallas = con_donacion.filter(F.col("puntos_calculados") != esperado_r1).count()
registrar("A", f"R4 donaciones no suman ({con_donacion.count()} tickets con donacion)", 0, fallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### R7 + R20 · Canales excluidos y cuentas de Honduras no acumulan
# MAGIC
# MAGIC La prueba es interesante porque **sí hay** tickets de Call Center o WhatsApp donde el agente
# MAGIC anotó el código del cliente, y **sí hay** compras de clientes de Honduras. Ninguno debe
# MAGIC tener movimientos.

# COMMAND ----------

excluidos = ["PEDIDOSYA", "UBEREATS", "CALLCENTER", "WHATSAPP"]
tentadores = t.filter(F.col("canal").isin(excluidos) & F.col("customer_id").isNotNull())
fallas = tentadores.join(mov, "transaccion_id").count()
registrar("A", f"R7 canales excluidos con cliente identificado ({tentadores.count()}) no acumulan", 0, fallas)

hn = clientes.filter(F.col("pais") == "HN").select("customer_id")
compras_hn = t.join(hn, "customer_id").count()
fallas = mov.join(hn, "customer_id").count()
registrar("A", f"R20 cuentas de Honduras ({compras_hn} compras) sin movimientos", 0, fallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### R8 · Tope de 1,000 puntos por día
# MAGIC
# MAGIC Esperado: en cada día, lo otorgado es `min(1000, suma calculada)`.

# COMMAND ----------

por_dia = (mov.filter(F.col("tipo") == "ACUMULACION")
              .groupBy("customer_id", "fecha_gt")
              .agg(F.sum("puntos").alias("otorgado"), F.sum("puntos_calculados").alias("calculado")))
# los tickets 100 % recortados no generan movimiento: lo calculado se suma desde las compras
dias = (t.filter(F.col("estado") == "COMPLETADA").join(clientes.filter(F.col("pais") == "GT"), "customer_id")
         .filter(~F.col("canal").isin(excluidos) & (F.col("monto_productos_q") > 0))
         .groupBy("customer_id", "fecha_gt").agg(F.sum(F.ceil(F.col("monto_productos_q") * 10)).alias("calculado_total"))
         .join(por_dia, ["customer_id", "fecha_gt"], "left").fillna(0, ["otorgado"]))
fallas = dias.filter(F.col("otorgado") != F.least(F.lit(1000), F.col("calculado_total"))).count()
topados = dias.filter(F.col("calculado_total") > 1000)
registrar("A", f"R8 otorgado = min(1000, calculado) en {dias.count()} cliente-dias", 0, fallas)

ej = topados.orderBy(F.desc("calculado_total")).first()
print(f"      ejemplo: {ej.customer_id} el {ej.fecha_gt} calculó {ej.calculado_total} y recibió {ej.otorgado}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### R10 · Cada lote vence exactamente 365 días después
# MAGIC
# MAGIC Y lo vencido nunca supera lo que el lote tenía.

# COMMAND ----------

venc = lotes.filter(F.col("tipo") == "VENCIMIENTO")
fallas = venc.filter(F.col("fecha_gt") != F.date_add("fecha_lote", 365)).count()
registrar("A", f"R10 vencimientos a los 365 dias ({venc.count()} lotes vencidos)", 0, fallas)

abonos = (mov.filter(F.col("puntos") > 0).groupBy("customer_id", F.col("fecha_gt").alias("fecha_lote"))
             .agg(F.sum("puntos").alias("abonado")))
fallas = (venc.groupBy("customer_id", "fecha_lote").agg((-F.sum("puntos")).alias("vencido"))
              .join(abonos, ["customer_id", "fecha_lote"], "left")
              .filter(F.col("abonado").isNull() | (F.col("vencido") > F.col("abonado"))).count())
registrar("A", "R10 lo vencido nunca supera lo abonado ese dia", 0, fallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### R14 · Un canje cuenta aunque la orden se anule
# MAGIC
# MAGIC Esperado: cada canje con costo en una orden anulada tiene su movimiento `CANJE` en el ledger.

# COMMAND ----------

canjes_anulados = (lineas.filter((F.col("tipo_linea") == "CANJE") & (F.col("recompensa_id") != "RW-BIENVENIDA"))
                         .join(t.filter((F.col("estado") == "ANULADA") & F.col("customer_id").isNotNull())
                                .join(clientes.filter(F.col("pais") == "GT").select("customer_id"), "customer_id")
                                .select("transaccion_id"), "transaccion_id"))
en_ledger = mov.filter(F.col("tipo") == "CANJE").select("transaccion_id", "recompensa_id")
fallas = canjes_anulados.join(en_ledger, ["transaccion_id", "recompensa_id"], "left_anti").count()
registrar("A", f"R14 canjes en ordenes anuladas descontados ({canjes_anulados.count()} canjes)", 0, fallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### R16 · El costo del canje es el del catálogo **vigente ese día**
# MAGIC
# MAGIC El Big Tasty costaba 7,000 puntos hasta el 28-feb-2026 y 7,500 desde el 1-mar-2026.

# COMMAND ----------

bt = mov.filter(F.col("recompensa_id") == "RW-BIGTASTY")
esperado_bt = F.when(F.col("fecha_gt") <= F.lit("2026-02-28").cast("date"), -7000).otherwise(-7500)
fallas = bt.filter(F.col("puntos") != esperado_bt).count()
antes = bt.filter(F.col("fecha_gt") <= F.lit("2026-02-28").cast("date")).count()
registrar("A", f"R16 Big Tasty: {antes} canjes a 7,000 y {bt.count() - antes} a 7,500", 0, fallas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### R17 · Bienvenida: 1,000 puntos el día de la primera compra que acumula

# COMMAND ----------

primera = (mov.filter(F.col("tipo") == "ACUMULACION").groupBy("customer_id").agg(F.min("fecha_hora_gt").alias("primera")))
bienv = mov.filter(F.col("tipo") == "BIENVENIDA")
fallas = (primera.join(bienv.select("customer_id", "fecha_hora_gt", "puntos"), "customer_id", "left")
                 .filter(F.col("puntos").isNull() | (F.col("puntos") != 1000)
                         | (F.col("fecha_hora_gt") != F.col("primera"))).count())
registrar("A", f"R17 una bienvenida de 1,000 en la primera compra ({bienv.count()} clientes)", 0, fallas)
registrar("A", "R17 bienvenidas = clientes que acumularon al menos una vez", primera.count(), bienv.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### R21 · Migración del programa anterior (saldo × 10, supuesto H1)

# COMMAND ----------

leg = (spark.table("silver_legado").filter(F.col("estado_migracion") == "REGISTRADO")
            .join(clientes.filter(F.col("pais") == "GT").select("customer_id"), "customer_id"))
migr = mov.filter(F.col("tipo") == "MIGRACION").select("customer_id", "puntos")
fallas = (leg.join(migr, "customer_id", "left")
             .filter(F.col("puntos").isNull() | (F.col("puntos") != F.col("saldo_puntos_legado") * 10)).count())
registrar("A", f"R21 migracion = saldo anterior x 10 ({leg.count()} cuentas)", 0, fallas)

pendientes = spark.table("silver_legado").filter(F.col("estado_migracion") == "PENDIENTE_REGISTRO").count()
print(f"      {pendientes} saldos del programa anterior siguen sin reclamar (el cliente no se registró)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B · Calidad de datos: ¿qué errores atrapó Silver?
# MAGIC
# MAGIC No basta con que los **conteos** coincidan: dos errores distintos podrían compensarse. Aquí se
# MAGIC comparan los **registros**: para cada error se cruza el conjunto detectado con el inyectado.
# MAGIC
# MAGIC - **Aciertos:** detectados que sí tenían el error.
# MAGIC - **Faltantes:** tenían el error y Silver no los vio (el error llegó a Gold).
# MAGIC - **Falsas alarmas:** Silver marcó algo que estaba bien.
# MAGIC
# MAGIC Es la misma lógica de *precision* y *recall* de un modelo de clasificación, aplicada al
# MAGIC pipeline. Los pedidos que tenían dos errores a la vez (E8 o E9 **y** E11) se esperan solo en
# MAGIC cuarentena.

# COMMAND ----------

with open(f"{CONTROL}/errores_inyectados.json", encoding="utf-8") as f:
    inyectados = {k.split("_")[0]: set(v) for k, v in json.load(f).items()}

e11_ids = {x.split(":")[0] for x in inyectados["E11"]}
b_cli = spark.table("bronze_clientes")
b_app = spark.table("bronze_app_pedidos")
cuarentena = spark.table("silver_cuarentena")
pos_t = t.filter(F.col("origen") == "POS")
app_t = t.filter(F.col("origen") == "APP")
validos_app = b_app.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
                           & F.col("store_id").isNotNull() & F.col("created_at").isNotNull())


def ids(df, columna):
    return {r[0] for r in df.select(columna).distinct().collect()}


detectados = {
    "E2": ids(pos_t.filter("canal_corregido"), "transaccion_id"),
    "E3": ids(pos_t.filter("precio_con_coma"), "transaccion_id"),
    "E4": ids(pos_t.filter("codigo_corregido"), "transaccion_id"),
    "E5": ids(pos_t.filter(F.col("codigo_lealtad_valido") == False), "transaccion_id"),  # noqa: E712
    "E6": ids(t.filter(~F.col("total_cuadra")), "transaccion_id"),
    "E7": ids(pos_t.filter(F.col("estado") == "ANULADA"), "transaccion_id"),
    "E8": ids(validos_app.groupBy("order_id", "status", "updated_at").count().filter("count > 1"), "order_id"),
    "E9": ids(app_t.filter(F.col("estado") == "ANULADA"), "transaccion_id"),
    "E10": ids(spark.table("gold_violaciones_reglas").filter(F.col("regla").startswith("R15")), "transaccion_id"),
    "E11": {f"{r.id_registro}:{r.motivo.split(':')[1]}" for r in cuarentena.filter(F.col("fuente") == "app").collect()},
    "E12": ids(b_cli.join(clientes, "customer_id").filter(F.col("email") != F.col("correo")), "customer_id"),
    "E13": {f"{r.customer_id}~{r.cuenta_principal_id}" for r in
            clientes.filter(F.col("sospecha_duplicado") & (F.col("customer_id") != F.col("cuenta_principal_id"))).collect()},
    "E14": ids(b_cli.join(clientes, "customer_id").filter(F.col("phone") != F.col("telefono")), "customer_id"),
    "E15": ids(clientes.filter(F.col("pais") == "HN"), "customer_id"),
    "E16": ids(spark.table("silver_restaurantes").filter("departamento_corregido"), "restaurante_id"),
    "E17": ids(spark.table("silver_legado").filter(F.col("estado_migracion") == "PENDIENTE_REGISTRO"), "correo"),
    "E18": ids(cuarentena.filter(F.col("fuente") == "legado"), "id_registro"),
}
esperados = dict(inyectados)
esperados["E8"] = inyectados["E8"] - e11_ids
esperados["E9"] = inyectados["E9"] - e11_ids

filas_b = []
for error in sorted(detectados, key=lambda e: int(e[1:])):
    esp, det = esperados[error], detectados[error]
    filas_b.append((error, len(esp), len(det), len(esp & det), len(esp - det), len(det - esp)))
    registrar("B", f"{error} registros detectados = registros inyectados ({len(esp)})",
              "0 faltantes / 0 falsas alarmas", f"{len(esp - det)} faltantes / {len(det - esp)} falsas alarmas")

# E1 (archivo reenviado) se detecta por contenido: se compara el número de filas duplicadas
filas_reenvio = (spark.table("bronze_pos_lineas")
                      .withColumn("archivo", F.element_at(F.split("_archivo_origen", "/"), -1))
                      .filter(F.col("archivo").isin(list(inyectados["E1"]))).count())
e1_detectado = spark.table("silver_calidad_detecciones").filter(F.col("error") == "E1").first()["detectado"]
registrar("B", f"E1 filas de {len(inyectados['E1'])} archivos reenviados eliminadas", filas_reenvio, e1_detectado)

display(pd.DataFrame(filas_b, columns=["error", "inyectados", "detectados", "aciertos", "faltantes", "falsas_alarmas"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C · Saldos: Gold contra la hoja de respuestas
# MAGIC
# MAGIC El generador llevó su propia contabilidad en **Python puro** y Gold la calculó en **Spark**,
# MAGIC con código completamente distinto. Si coinciden para cada cliente en cada métrica, es muy
# MAGIC improbable que las dos estén mal de la misma forma.

# COMMAND ----------

esperado_saldos = pd.read_csv(f"{CONTROL}/saldos_esperados.csv")
gold_saldos = spark.table("gold_saldos").toPandas()
METRICAS = ["puntos_acumulados", "puntos_bienvenida", "puntos_migrados", "puntos_canjeados",
            "puntos_vencidos", "puntos_perdidos_por_tope", "saldo_final"]

cruce = esperado_saldos.merge(gold_saldos, on="customer_id", how="outer",
                              suffixes=("_esperado", "_gold"), indicator=True)
registrar("C", "clientes presentes en ambos lados", len(esperado_saldos), int((cruce["_merge"] == "both").sum()))

filas_c = []
for m in METRICAS:
    esp = cruce[f"{m}_esperado"].fillna(-1).astype("int64")
    gol = cruce[f"{m}_gold"].fillna(-1).astype("int64")
    distintos = int((esp != gol).sum())
    filas_c.append((m, int(esp.sum()), int(gol.sum()), distintos))
    registrar("C", f"{m}: clientes con diferencia", 0, distintos)

display(pd.DataFrame(filas_c, columns=["metrica", "total_esperado", "total_gold", "clientes_con_diferencia"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D · ¿El recomendador aprendió los gustos reales?
# MAGIC
# MAGIC El modelo **nunca vio** el segmento de gustos de los clientes: solo sus compras y canjes. Aquí
# MAGIC se revela el segmento real y se mide si entre sus 3 recomendaciones hay alguna recompensa que
# MAGIC le gusta, comparado con recomendar a todos las 3 más populares.
# MAGIC
# MAGIC Recompensas que le gustan a cada segmento (según los perfiles del generador v2):

# COMMAND ----------

RECOMPENSAS_DEL_SEGMENTO = {
    "RES": {"RW-BIGMAC", "RW-BIGTASTY", "RW-QUESOB"},
    "POLLO": {"RW-MCPOLLO", "RW-NUGGETS6"},
    "DESAYUNO": {"RW-MCMUFFIN", "RW-DESAYUNO-DLX", "RW-CAFE"},
    "CAFE_POSTRE": {"RW-CAFE", "RW-MCFLURRY"},
    "FAMILIA": {"RW-MCFLURRY"},
}

segmentos = pd.read_csv(f"{CONTROL}/segmentos_clientes.csv")
reco = spark.table("gold_recomendaciones").toPandas()
top3 = reco.groupby("customer_id")["recompensa_id"].apply(set).rename("top3")
top1 = reco[reco["posicion"] == 1].set_index("customer_id")["recompensa_id"].rename("top1")

populares = (spark.table("gold_movimientos_puntos").filter(F.col("tipo") == "CANJE")
                  .join(spark.table("silver_catalogo").filter(F.col("vigente_hasta").isNull()).select("recompensa_id"),
                        "recompensa_id")
                  .groupBy("recompensa_id").count().orderBy(F.desc("count")).limit(3).toPandas())
top3_popular = set(populares["recompensa_id"])
print(f"baseline: las 3 más canjeadas y vigentes = {sorted(top3_popular)}")

ev = segmentos.set_index("customer_id").join(top3, how="inner").join(top1)
ev["gustos"] = ev["segmento"].map(RECOMPENSAS_DEL_SEGMENTO)
ev["modelo_top3"] = [len(g & r) > 0 for g, r in zip(ev["gustos"], ev["top3"])]
ev["popular_top3"] = [len(g & top3_popular) > 0 for g in ev["gustos"]]
ev["modelo_top1"] = [r in g for g, r in zip(ev["gustos"], ev["top1"])]

por_segmento = (ev.groupby("segmento")[["modelo_top3", "popular_top3", "modelo_top1"]].mean().round(3)
                  .assign(clientes=ev.groupby("segmento").size()).reset_index())
display(por_segmento)

modelo = round(ev["modelo_top3"].mean(), 3)
popular = round(ev["popular_top3"].mean(), 3)
registrar("D", f"top-3 del modelo incluye algo de su gusto ({len(ev)} clientes) > baseline popular",
          f"> {popular}", modelo, ok=modelo > popular)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de la POC
# MAGIC
# MAGIC Todas las pruebas quedan en `eval_resultados_poc`, para citarlas en el reporte.

# COMMAND ----------

resumen = spark.createDataFrame(pd.DataFrame(resultados, columns=["parte", "prueba", "esperado", "obtenido", "ok"]))
escribir_tabla(resumen, "eval_resultados_poc",
               "Resultados de la prueba funcional (POC) contra la hoja de respuestas.")

total, ok = len(resultados), sum(r[4] for r in resultados)
print(f"\n{ok} de {total} pruebas OK")
for parte in "ABCD":
    subtotal = [r for r in resultados if r[0] == parte]
    print(f"  Parte {parte}: {sum(r[4] for r in subtotal)} / {len(subtotal)}")

assert ok == total, "Hay pruebas de la POC que fallaron"
