# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Recomendador de recompensas (Fase 4 · Modelado)
# MAGIC
# MAGIC Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala
# MAGIC
# MAGIC Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis
# MAGIC
# MAGIC **Pregunta de negocio:** ¿qué recompensa le mostramos a cada cliente en la app para que
# MAGIC vuelva a comprar y canjee? Es lo que McDonald's hace con Dynamic Yield.
# MAGIC
# MAGIC **Cómo encaja en el pipeline:** es un paso más después de Gold. Lee tablas Silver y Gold,
# MAGIC entrena y evalúa un modelo con **MLflow**, y escribe `gold_recomendaciones`, que la app o el
# MAGIC CRM usan para la campaña.
# MAGIC
# MAGIC ```
# MAGIC silver_lineas ─┐
# MAGIC silver_catalogo├─► variables por (cliente, recompensa) ─► modelo ─► gold_recomendaciones ─► app / CRM
# MAGIC gold_movimientos┘                                           │
# MAGIC                                                          MLflow (métricas, modelo versionado)
# MAGIC ```
# MAGIC
# MAGIC Diseño **de dos etapas**, como los recomendadores reales:
# MAGIC 1. **Candidatos:** las recompensas vigentes en el catálogo ese día.
# MAGIC 2. **Ranking:** un modelo ordena los candidatos de cada cliente combinando **gusto** (qué
# MAGIC    compra), **costo** (¿le alcanzan los puntos?), **hora** (¿desayuna?) y **popularidad**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración y ventanas de tiempo
# MAGIC
# MAGIC La trampa más peligrosa aquí es la **fuga de información**, la misma del Ejercicio 1 (donde
# MAGIC `score` y `winner` revelaban el resultado). Si las variables usan datos de **después** de la
# MAGIC fecha en que se hace la recomendación, el modelo "ve el futuro" y sus métricas mienten.
# MAGIC
# MAGIC Por eso cada conjunto se arma con un **corte**: variables solo con datos *antes* del corte y
# MAGIC etiqueta = qué canjeó en los 92 días *después*.
# MAGIC
# MAGIC | Conjunto | Variables hasta | Etiqueta (canjes entre) | Uso |
# MAGIC |---|---|---|---|
# MAGIC | Entrenamiento | 20-mar-2026 | 20-mar → 20-jun | Entrenar |
# MAGIC | Prueba | 20-jun-2026 | 20-jun → 20-sep | Evaluar con datos **que el modelo nunca vio** |
# MAGIC | Producción | 20-sep-2026 (corte) | — (es el futuro) | Recomendar |

# COMMAND ----------

from datetime import timedelta

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.models import infer_signature
from pyspark.sql import Window
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

CATALOGO = "workspace"
ESQUEMA = "mimcdonalds"
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

SEMILLA = 42
VENTANA_DIAS = 92
MIN_UNIDADES_HISTORIA = 3          # sin historia no hay gustos que aprender (arranque en frío)
TOP_K = 3

def escribir_gold(df, tabla, descripcion):
    nombre = f"{CATALOGO}.{ESQUEMA}.{tabla}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre)
    spark.sql(f"COMMENT ON TABLE {nombre} IS '{descripcion}'")
    filas = spark.table(nombre).count()
    print(f"[gold] {tabla:<24} {filas:>7,} filas")
    return filas


FECHA_CORTE = spark.table("silver_transacciones").agg(F.max("fecha_gt")).first()[0]
CORTE_PRUEBA = FECHA_CORTE - timedelta(days=VENTANA_DIAS)
CORTE_ENTRENAMIENTO = CORTE_PRUEBA - timedelta(days=VENTANA_DIAS)
print(f"entrenamiento: variables < {CORTE_ENTRENAMIENTO} · etiqueta hasta {CORTE_PRUEBA}")
print(f"prueba:        variables < {CORTE_PRUEBA} · etiqueta hasta {FECHA_CORTE}")
print(f"producción:    variables < {FECHA_CORTE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datos de entrada
# MAGIC
# MAGIC - **Categoría de cada producto:** una tabla de referencia del menú (conocimiento del negocio).
# MAGIC   Permite aprender gustos amplios: quien compra McPollo seguramente también quiere Nuggets.
# MAGIC - **Compras:** líneas de producto de tickets completados de clientes de Guatemala.
# MAGIC - **Canjes:** líneas de canje, sin la quesoburguesa de bienvenida (no se "recomienda": se regala).

# COMMAND ----------

CATEGORIA_PRODUCTO = {
    "P-COMBO-BIGMAC": "res", "P-COMBO-CUARTO": "res", "P-BIGMAC": "res", "P-BIGTASTY": "res",
    "P-QUESOB": "res", "P-HAMB": "res",
    "P-COMBO-MCPOLLO": "pollo", "P-MCPOLLO": "pollo", "P-NUGGETS6": "pollo", "P-NUGGETS10": "pollo",
    "P-MCMUFFIN": "desayuno", "P-DESAYUNO-DLX": "desayuno", "P-HOTCAKES": "desayuno",
    "P-CAFE-AMER": "cafe_postre", "P-CAPUCHINO": "cafe_postre", "P-PASTEL-MANZ": "cafe_postre",
    "P-MCFLURRY": "cafe_postre", "P-SUNDAE": "cafe_postre", "P-CONO": "cafe_postre",
    "P-CAJITA": "familia",
    "P-PAPAS-M": "acompanamiento", "P-PAPAS-G": "acompanamiento", "P-GASEOSA-M": "acompanamiento",
}
categorias = spark.createDataFrame(list(CATEGORIA_PRODUCTO.items()), "producto_id STRING, categoria STRING")

clientes_gt = spark.table("silver_clientes").filter(F.col("pais") == "GT").select("customer_id")
transacciones = spark.table("silver_transacciones")
lineas = spark.table("silver_lineas")

compras = (
    lineas.filter(F.col("tipo_linea") == "PRODUCTO")
          .join(transacciones.filter(F.col("estado") == "COMPLETADA")
                             .select("transaccion_id", "customer_id", "fecha_gt", "franja"), "transaccion_id")
          .join(clientes_gt, "customer_id")
          .join(F.broadcast(categorias), "producto_id", "left")
)

canjes = (
    lineas.filter((F.col("tipo_linea") == "CANJE") & (F.col("recompensa_id") != "RW-BIENVENIDA"))
          .join(transacciones.select("transaccion_id", "customer_id", "fecha_gt"), "transaccion_id")
          .join(clientes_gt, "customer_id")
          .select("customer_id", "fecha_gt", "recompensa_id")
)

movimientos = spark.table("gold_movimientos_puntos")
catalogo = spark.table("silver_catalogo").filter(F.col("puntos") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Variables por (cliente, recompensa candidata)
# MAGIC
# MAGIC Una sola función arma las variables **para cualquier corte**. Así, entrenamiento, prueba y
# MAGIC producción usan **exactamente el mismo código**: si se calcularan distinto, el modelo vería en
# MAGIC producción algo diferente a lo que aprendió (*training-serving skew*).
# MAGIC
# MAGIC | Variable | Qué mide | Idea |
# MAGIC |---|---|---|
# MAGIC | `share_producto` | % de las compras del cliente que son ese producto | Gusto específico |
# MAGIC | `share_categoria` | % de sus compras en esa categoría | Gusto amplio |
# MAGIC | `canjes_previos` | Veces que ya canjeó esa recompensa | Hábito |
# MAGIC | `popularidad` | % de los canjes de todos en los últimos 180 días | Lo que funciona en general |
# MAGIC | `costo`, `saldo`, `alcanza` | Puntos que cuesta, puntos que tiene y si le alcanzan | Factibilidad |
# MAGIC | `puntos_mes` | Cuántos puntos gana al mes (últimos 90 días) | Qué tan pronto llegará |
# MAGIC | `pct_desayuno`, `es_desayuno`, `afinidad_desayuno` | Si desayuna y si la recompensa es de desayuno | Los desayunos solo se canjean temprano |
# MAGIC
# MAGIC El **saldo al corte** sale del ledger de Gold sumando solo movimientos anteriores al corte:
# MAGIC otra razón para guardar movimientos y no solo el saldo final.

# COMMAND ----------

VARIABLES = ["share_producto", "share_categoria", "canjes_previos", "popularidad", "costo",
             "saldo", "alcanza", "puntos_mes", "pct_desayuno", "es_desayuno", "afinidad_desayuno"]


def variables(corte):
    historia = compras.filter(F.col("fecha_gt") < F.lit(corte))

    por_cliente = historia.groupBy("customer_id").agg(F.sum("cantidad").alias("unidades"))
    por_producto = historia.groupBy("customer_id", "producto_id").agg(F.sum("cantidad").alias("u_producto"))
    por_categoria = historia.groupBy("customer_id", "categoria").agg(F.sum("cantidad").alias("u_categoria"))
    desayuno = (historia.select("customer_id", "transaccion_id", "franja").distinct()
                        .groupBy("customer_id")
                        .agg(F.avg((F.col("franja") == "DESAYUNO").cast("double")).alias("pct_desayuno")))
    gasto_90 = (historia.filter(F.col("fecha_gt") >= F.date_sub(F.lit(corte), 90))
                        .groupBy("customer_id")
                        .agg((F.sum(F.col("cantidad") * F.col("precio_unitario_q")) * 10 / 3).alias("puntos_mes")))
    previos = (canjes.filter(F.col("fecha_gt") < F.lit(corte))
                     .groupBy("customer_id", "recompensa_id").agg(F.count("*").alias("canjes_previos")))
    recientes = canjes.filter((F.col("fecha_gt") < F.lit(corte)) & (F.col("fecha_gt") >= F.date_sub(F.lit(corte), 180)))
    popularidad = (recientes.groupBy("recompensa_id").count()
                            .withColumn("popularidad", F.col("count") / F.sum("count").over(Window.partitionBy())))
    saldo = (movimientos.filter(F.col("fecha_gt") < F.lit(corte))
                        .groupBy("customer_id").agg(F.sum("puntos").alias("saldo")))

    candidatos = (catalogo.filter((F.col("vigente_desde") <= F.lit(corte))
                                  & (F.coalesce("vigente_hasta", F.lit("9999-12-31").cast("date")) >= F.lit(corte)))
                          .join(F.broadcast(categorias), "producto_id", "left")
                          .select("recompensa_id", "producto_id", "producto", "categoria",
                                  F.col("puntos").alias("costo")))

    pares = (
        por_cliente.filter(F.col("unidades") >= MIN_UNIDADES_HISTORIA)
                   .crossJoin(F.broadcast(candidatos))
                   .join(por_producto, ["customer_id", "producto_id"], "left")
                   .join(por_categoria, ["customer_id", "categoria"], "left")
                   .join(previos, ["customer_id", "recompensa_id"], "left")
                   .join(F.broadcast(popularidad.select("recompensa_id", "popularidad")), "recompensa_id", "left")
                   .join(desayuno, "customer_id", "left")
                   .join(gasto_90, "customer_id", "left")
                   .join(saldo, "customer_id", "left")
                   .fillna(0, subset=["u_producto", "u_categoria", "canjes_previos", "popularidad",
                                      "pct_desayuno", "puntos_mes", "saldo"])
                   .withColumn("share_producto", F.col("u_producto") / F.col("unidades"))
                   .withColumn("share_categoria", F.col("u_categoria") / F.col("unidades"))
                   .withColumn("alcanza", (F.col("saldo") >= F.col("costo")).cast("int"))
                   .withColumn("es_desayuno", (F.col("categoria") == "desayuno").cast("int"))
                   .withColumn("afinidad_desayuno", F.col("pct_desayuno") * F.col("es_desayuno"))
                   .withColumn("fecha_corte", F.lit(corte))
    )
    # todo a double: toPandas() convertiría los decimal de Spark en objetos Decimal de Python
    for v in VARIABLES:
        pares = pares.withColumn(v, F.col(v).cast("double"))
    return pares


def con_etiqueta(df, corte):
    """y = 1 si el cliente canjeó esa recompensa en los 92 días siguientes al corte.

    Solo se evalúan recompensas que existieron durante TODA la ventana: no tiene sentido
    castigar al modelo por no acertar algo que salió del catálogo a mitad de camino (el café
    salió el 30-jun). Ojo: saber que una recompensa va a salir es información del futuro, así
    que este filtro se usa solo para la etiqueta y la evaluación, NUNCA para las variables.
    """
    fin = F.date_add(F.lit(corte), VENTANA_DIAS - 1)
    disponibles = catalogo.filter((F.col("vigente_desde") <= F.lit(corte))
                                  & (F.coalesce("vigente_hasta", F.lit("9999-12-31").cast("date")) >= fin)) \
                          .select("recompensa_id").distinct()
    futuros = (canjes.filter((F.col("fecha_gt") >= F.lit(corte))
                             & (F.col("fecha_gt") < F.date_add(F.lit(corte), VENTANA_DIAS)))
                     .join(disponibles, "recompensa_id")
                     .select("customer_id", "recompensa_id").distinct().withColumn("y", F.lit(1)))
    quienes = futuros.select("customer_id").distinct()      # solo clientes que sí canjearon algo
    return (df.join(disponibles, "recompensa_id")
              .join(quienes, "customer_id")
              .join(futuros, ["customer_id", "recompensa_id"], "left")
              .fillna(0, ["y"]))


entrenamiento = con_etiqueta(variables(CORTE_ENTRENAMIENTO), CORTE_ENTRENAMIENTO).toPandas()
prueba = con_etiqueta(variables(CORTE_PRUEBA), CORTE_PRUEBA).toPandas()
for nombre, df in [("entrenamiento", entrenamiento), ("prueba", prueba)]:
    print(f"{nombre:<14} {df.customer_id.nunique():>5,} clientes · {len(df):>6,} pares · "
          f"{df.y.mean():.1%} positivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Baselines primero
# MAGIC
# MAGIC Antes de cualquier modelo, dos estrategias sin ML. Si el modelo no les gana, **no vale la
# MAGIC pena**: es más caro de mantener y no aporta. (En la v1 de los datos, justamente, nada les
# MAGIC ganaba.)
# MAGIC
# MAGIC **Métrica: `hit@k`**, el porcentaje de clientes para los que al menos una de las `k`
# MAGIC recompensas recomendadas fue la que después canjeó. `hit@1` es la más exigente: equivale a
# MAGIC "acertó con la recompensa del banner principal".

# COMMAND ----------

def hit_at_k(df, columna_puntaje, k):
    top = (df.sort_values(["customer_id", columna_puntaje], ascending=[True, False])
             .groupby("customer_id").head(k))
    return top.groupby("customer_id")["y"].max().mean()


prueba["baseline_popularidad"] = prueba["popularidad"]
prueba["baseline_frecuencia"] = prueba["share_producto"] + 1e-3 * prueba["popularidad"]

resultados = {}
for nombre in ["baseline_popularidad", "baseline_frecuencia"]:
    resultados[nombre] = {f"hit_at_{k}": hit_at_k(prueba, nombre, k) for k in (1, 3)}
    print(f"{nombre:<24} hit@1 {resultados[nombre]['hit_at_1']:.3f} · hit@3 {resultados[nombre]['hit_at_3']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Modelos, registrados en MLflow
# MAGIC
# MAGIC Se prueban dos modelos de clasificación: cada par (cliente, recompensa) recibe la
# MAGIC probabilidad de ser canjeado, y se ordena por esa probabilidad.
# MAGIC
# MAGIC - **Regresión logística:** simple e interpretable.
# MAGIC - **Gradient boosting:** captura combinaciones, por ejemplo "le gusta **y** le alcanza".
# MAGIC
# MAGIC Cada entrenamiento es un *run* de **MLflow** con sus parámetros, métricas y el modelo. En
# MAGIC Databricks se ven en el ícono de experimentos (matraz) a la derecha del notebook: así se
# MAGIC compara y se reproduce cualquier versión.

# COMMAND ----------

modelos = {
    "regresion_logistica": make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000)),
    "gradient_boosting": HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05,
                                                        random_state=SEMILLA),
}

for nombre, modelo in modelos.items():
    with mlflow.start_run(run_name=nombre):
        modelo.fit(entrenamiento[VARIABLES], entrenamiento["y"])
        prueba[nombre] = modelo.predict_proba(prueba[VARIABLES])[:, 1]
        resultados[nombre] = {f"hit_at_{k}": hit_at_k(prueba, nombre, k) for k in (1, 3)}

        mlflow.log_params({"modelo": nombre, "ventana_dias": VENTANA_DIAS,
                           "corte_entrenamiento": str(CORTE_ENTRENAMIENTO), "corte_prueba": str(CORTE_PRUEBA),
                           "variables": ",".join(VARIABLES)})
        mlflow.log_metrics(resultados[nombre])
        for base in ["baseline_popularidad", "baseline_frecuencia"]:
            mlflow.log_metric(f"{base}_hit_at_1", resultados[base]["hit_at_1"])
        print(f"{nombre:<24} hit@1 {resultados[nombre]['hit_at_1']:.3f} · hit@3 {resultados[nombre]['hit_at_3']:.3f}")

tabla = pd.DataFrame(resultados).T
tabla["mejora_hit_at_1_vs_popularidad"] = tabla["hit_at_1"] / tabla.loc["baseline_popularidad", "hit_at_1"] - 1
display(tabla.round(3).reset_index().rename(columns={"index": "estrategia"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ¿Qué aprendió el modelo?
# MAGIC
# MAGIC Un modelo que no se puede explicar es difícil de defender ante el cliente. La **importancia
# MAGIC por permutación** mide cuánto empeora `hit@1` si se desordena una variable: si empeora mucho,
# MAGIC el modelo depende de ella.

# COMMAND ----------

mejor = max(["regresion_logistica", "gradient_boosting"], key=lambda m: resultados[m]["hit_at_1"])
modelo_final = modelos[mejor]
print(f"mejor modelo en prueba: {mejor}")

importancia = []
base_hit = resultados[mejor]["hit_at_1"]
for variable in VARIABLES:
    desordenado = prueba.copy()
    desordenado[variable] = desordenado[variable].sample(frac=1, random_state=SEMILLA).values
    desordenado["p"] = modelo_final.predict_proba(desordenado[VARIABLES])[:, 1]
    importancia.append((variable, base_hit - hit_at_k(desordenado, "p", 1)))

display(pd.DataFrame(importancia, columns=["variable", "caida_hit_at_1"])
          .sort_values("caida_hit_at_1", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Modelo de producción
# MAGIC
# MAGIC Para producción se reentrena el mejor modelo con **las dos ventanas etiquetadas** (más datos,
# MAGIC y los más recientes) y se registra en **Unity Catalog** como
# MAGIC `workspace.mimcdonalds.recomendador_recompensas`. Queda versionado junto a las tablas, con su
# MAGIC linaje: cada versión sabe con qué datos y métricas se entrenó.

# COMMAND ----------

completo = pd.concat([entrenamiento, prueba], ignore_index=True)
modelo_final.fit(completo[VARIABLES], completo["y"])

mlflow.set_registry_uri("databricks-uc")
NOMBRE_MODELO = f"{CATALOGO}.{ESQUEMA}.recomendador_recompensas"
with mlflow.start_run(run_name=f"produccion_{mejor}"):
    mlflow.log_params({"modelo": mejor, "entrenado_hasta": str(FECHA_CORTE), "variables": ",".join(VARIABLES)})
    mlflow.log_metrics({f"prueba_{k}": v for k, v in resultados[mejor].items()})
    firma = infer_signature(completo[VARIABLES], modelo_final.predict_proba(completo[VARIABLES])[:, 1])
    # cloudpickle: funciona con cualquier versión de MLflow. (Las versiones nuevas usan por defecto
    # `skops`, más seguro, que aún no acepta los árboles de HistGradientBoosting.) Un pickle puede
    # ejecutar código al cargarse: está bien aquí porque el modelo lo creamos nosotros.
    info = mlflow.sklearn.log_model(modelo_final, artifact_path="modelo", signature=firma,
                                    input_example=completo[VARIABLES].head(3),
                                    serialization_format="cloudpickle",
                                    registered_model_name=NOMBRE_MODELO)
print(f"modelo registrado: {NOMBRE_MODELO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Recomendaciones → `gold_recomendaciones`
# MAGIC
# MAGIC Se calculan las variables **al corte de hoy** y se guardan las 3 mejores recompensas de cada
# MAGIC cliente, con un mensaje listo para la app. El mensaje usa el saldo real del ledger: "te faltan
# MAGIC 500 puntos" es más accionable que "te recomendamos un McFlurry".
# MAGIC
# MAGIC Solo se recomiendan recompensas **vigentes hoy**: el café salió del catálogo en junio, así que
# MAGIC ya no aparece aunque el cliente lo haya canjeado antes.

# COMMAND ----------

actuales = variables(FECHA_CORTE).toPandas()
actuales["puntaje"] = modelo_final.predict_proba(actuales[VARIABLES])[:, 1]
actuales["posicion"] = (actuales.groupby("customer_id")["puntaje"]
                                .rank(method="first", ascending=False).astype(int))

top = actuales[actuales["posicion"] <= TOP_K][
    ["customer_id", "posicion", "recompensa_id", "producto", "costo", "saldo", "puntaje", "fecha_corte"]]

recomendaciones = (
    spark.createDataFrame(top)
         .withColumn("costo", F.col("costo").cast("long"))
         .withColumn("saldo", F.col("saldo").cast("long"))
         .withColumn("puntos_faltantes", F.greatest(F.lit(0), F.col("costo") - F.col("saldo")).cast("long"))
         .withColumn("mensaje",
                     F.when(F.col("puntos_faltantes") == 0,
                            F.format_string("¡Ya puedes canjear tu %s! Cuesta %,d puntos.", "producto", "costo"))
                      .otherwise(F.format_string("Te faltan %,d puntos para tu %s.", "puntos_faltantes", "producto")))
         .withColumn("modelo", F.lit(f"{NOMBRE_MODELO} ({mejor})"))
)

escribir_gold(recomendaciones, "gold_recomendaciones",
              "Top 3 recompensas por cliente segun el recomendador, con mensaje para la app.")

display(spark.table("gold_recomendaciones").filter("posicion = 1").orderBy(F.desc("puntaje")).limit(10))
display(spark.table("gold_recomendaciones").filter("posicion = 1")
             .groupBy("producto").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resultado
# MAGIC
# MAGIC - `gold_recomendaciones`: 3 recompensas por cliente con historia, con su mensaje.
# MAGIC - En MLflow: los *runs* de cada modelo con sus métricas contra los baselines.
# MAGIC - En Unity Catalog: el modelo `recomendador_recompensas`, versionado.
# MAGIC
# MAGIC La propuesta completa (cómo se opera, se monitorea y se reentrena) está en
# MAGIC `docs/04_modelado.md`. Si el recomendador aprendió los gustos **correctos** se evalúa en la
# MAGIC Fase 5, contra los segmentos reales que guardó el generador.
