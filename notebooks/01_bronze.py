# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Capa BRONZE — los datos tal como llegan
# MAGIC
# MAGIC Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala
# MAGIC
# MAGIC Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis
# MAGIC
# MAGIC Lee los archivos que dejó el generador en el Volume `landing` y crea **una tabla Delta por
# MAGIC fuente**. Bronze tiene **una sola regla: no corregir nada**.
# MAGIC
# MAGIC | Se hace | NO se hace |
# MAGIC |---|---|
# MAGIC | Leer cada archivo y guardarlo como tabla | Convertir tipos (`"22,00"` sigue siendo texto) |
# MAGIC | Guardar **todas las columnas como texto** | Quitar duplicados (el archivo reenviado entra completo) |
# MAGIC | Agregar de qué archivo vino cada fila y cuándo se ingirió | Filtrar filas malas o canales excluidos |
# MAGIC
# MAGIC ¿Por qué tan "tonta"? Porque Bronze es la **evidencia**. Si en Gold un saldo no cuadra, hay que
# MAGIC poder volver al dato **exactamente como lo mandó el sistema** y a su archivo de origen. Si
# MAGIC Bronze corrigiera algo, esa evidencia se perdería.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"
ESQUEMA = "mimcdonalds"
LANDING = f"/Volumes/{CATALOGO}/{ESQUEMA}/landing"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")
print(f"[setup] leyendo de {LANDING}")
print(f"[setup] escribiendo en {CATALOGO}.{ESQUEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. La función que escribe en Bronze
# MAGIC
# MAGIC Las 6 tablas se escriben igual, así que la lógica vive en **una sola función**:
# MAGIC
# MAGIC - **`_archivo_origen`**: la ruta exacta del archivo, tomada de la columna oculta `_metadata`
# MAGIC   que Databricks agrega a toda lectura de archivos. Hay que agregarla **justo después de leer**,
# MAGIC   antes de cualquier otra transformación. (La función vieja `input_file_name()` no funciona
# MAGIC   con Unity Catalog.)
# MAGIC - **`_fecha_ingesta`**: cuándo corrió esta carga. Si mañana se reprocesa, se sabe qué versión
# MAGIC   es cuál.
# MAGIC - **`overwrite`**: cada corrida reemplaza la tabla completa, así que correr el notebook dos
# MAGIC   veces da el mismo resultado (*idempotencia*). En producción se usaría **Auto Loader**, que solo
# MAGIC   lee los archivos nuevos.
# MAGIC - **`COMMENT ON TABLE`**: la descripción queda guardada en Unity Catalog, visible en *Catalog*.
# MAGIC   Es documentación que vive junto al dato, justo lo que le faltaba al sistema del cliente.
# MAGIC
# MAGIC Las columnas que empiezan con `_` son **metadatos del pipeline**, no datos del negocio.

# COMMAND ----------

def con_metadatos(df):
    """Se llama justo después de leer, mientras `_metadata` todavía está disponible."""
    return (df.withColumn("_archivo_origen", F.col("_metadata.file_path"))
              .withColumn("_fecha_ingesta", F.current_timestamp()))


def escribir_bronze(df, tabla, descripcion):
    nombre = f"{CATALOGO}.{ESQUEMA}.{tabla}"
    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(nombre))
    spark.sql(f"COMMENT ON TABLE {nombre} IS '{descripcion}'")
    filas = spark.table(nombre).count()
    print(f"[bronze] {tabla:<22} {filas:>7,} filas")
    return filas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fuente A · POS del restaurante (CSV)
# MAGIC
# MAGIC - `header=true`: la primera fila trae los nombres de las columnas.
# MAGIC - **Sin `inferSchema`**: Spark deja todo como texto. Si lo activáramos, intentaría convertir
# MAGIC   `precio_unitario_q` a número y las filas con `"22,00"` (E3) quedarían **nulas sin avisar**.
# MAGIC - Se lee la carpeta `pos/` completa. Como las subcarpetas se llaman `fecha=AAAA-MM-DD`, Spark
# MAGIC   las reconoce como **particiones** y agrega una columna `fecha`. La guardamos como
# MAGIC   `_particion_fecha` (texto) para dejar claro que viene de la carpeta, no del ticket.
# MAGIC - El **archivo reenviado** (E1) entra completo: aquí se ve el duplicado, y Silver lo resuelve.

# COMMAND ----------

pos = con_metadatos(
    spark.read
         .option("header", "true")
         .csv(f"{LANDING}/pos/")
)
pos = pos.withColumn("_particion_fecha", F.col("fecha").cast("string")).drop("fecha")

filas_pos = escribir_bronze(
    pos, "bronze_pos_lineas",
    "Lineas de tickets del POS de restaurantes, tal como llegan (CSV diario). Todo en texto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fuente B · App McDonald's GT (JSON Lines anidado)
# MAGIC
# MAGIC - `primitivesAsString=true`: números y booleanos del JSON quedan como texto, igual que en el
# MAGIC   POS. Así **todas** las tablas Bronze siguen la misma regla.
# MAGIC - Los `items` se quedan **anidados** (un arreglo dentro de cada pedido). Aplanarlos es
# MAGIC   transformar, así que le toca a Silver.
# MAGIC - Los pedidos a los que les falta un campo (E11) no fallan: Spark junta el esquema de todos los
# MAGIC   archivos y deja ese campo en nulo.
# MAGIC - Cada línea del archivo es un **evento**: un pedido cancelado (E9) aparece dos veces y un
# MAGIC   evento duplicado (E8) también. Bronze guarda todos.

# COMMAND ----------

app = con_metadatos(
    spark.read
         .option("primitivesAsString", "true")
         .json(f"{LANDING}/app/")
)
app = app.withColumn("_particion_fecha", F.col("fecha").cast("string")).drop("fecha")

filas_app = escribir_bronze(
    app, "bronze_app_pedidos",
    "Eventos de pedidos de la app (McDelivery y pickup), JSON anidado tal como llega. Fechas en UTC.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fuente C · CRM de clientes (un arreglo JSON)
# MAGIC
# MAGIC El CRM no manda una línea por cliente, sino **un solo arreglo** `[{...}, {...}]` repartido en
# MAGIC muchas líneas, como lo devolvería una API. Sin `multiLine=true`, Spark intentaría leer cada
# MAGIC línea como un JSON independiente y todo terminaría en `_corrupt_record`.

# COMMAND ----------

clientes = con_metadatos(
    spark.read
         .option("multiLine", "true")
         .option("primitivesAsString", "true")
         .json(f"{LANDING}/crm/clientes.json")
)

filas_clientes = escribir_bronze(
    clientes, "bronze_clientes",
    "Cuentas registradas en la app (CRM). Incluye cuentas de Honduras y correos sin normalizar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fuentes D, E y F · Maestros y programa anterior (CSV)

# COMMAND ----------

def leer_csv(ruta):
    return con_metadatos(spark.read.option("header", "true").csv(ruta))


filas_rest = escribir_bronze(
    leer_csv(f"{LANDING}/maestros/restaurantes.csv"), "bronze_restaurantes",
    "Maestro de restaurantes. Banderas SI/NO y departamentos sin normalizar.")

filas_cat = escribir_bronze(
    leer_csv(f"{LANDING}/maestros/catalogo_recompensas.csv"), "bronze_catalogo",
    "Catalogo de recompensas versionado (vigente_desde / vigente_hasta).")

filas_leg = escribir_bronze(
    leer_csv(f"{LANDING}/legado/puntos_mcdelivery.csv"), "bronze_legado",
    "Saldos del programa anterior Puntos McDelivery al 27-ago-2025 (1 pt por Q1). Fechas dd/mm/aaaa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Comprobaciones
# MAGIC
# MAGIC ### 7.1 ¿Llegó todo?
# MAGIC
# MAGIC Cada tabla debe tener **exactamente** las filas de sus archivos: Bronze no descarta nada.
# MAGIC Los números esperados se calcularon contando las filas de los archivos (semilla 42).

# COMMAND ----------

esperado = {
    "bronze_pos_lineas": 95_055,
    "bronze_app_pedidos": 7_977,
    "bronze_clientes": 5_250,
    "bronze_restaurantes": 30,
    "bronze_catalogo": 12,
    "bronze_legado": 1_817,
}
obtenido = {
    "bronze_pos_lineas": filas_pos, "bronze_app_pedidos": filas_app,
    "bronze_clientes": filas_clientes, "bronze_restaurantes": filas_rest,
    "bronze_catalogo": filas_cat, "bronze_legado": filas_leg,
}
for tabla, n in esperado.items():
    marca = "OK " if obtenido[tabla] == n else "REVISAR"
    print(f"{marca} {tabla:<22} esperado {n:>7,} · obtenido {obtenido[tabla]:>7,}")

assert obtenido == esperado, "Bronze no tiene las filas esperadas: revisar la lectura"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 ¿Todo quedó como texto?
# MAGIC
# MAGIC El esquema del POS debe ser **solo `string`**, salvo `_fecha_ingesta` (que es nuestra).

# COMMAND ----------

spark.table("bronze_pos_lineas").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Evidencia de que Bronze **no corrigió nada**
# MAGIC
# MAGIC Los errores inyectados siguen ahí, tal como llegaron. Silver los resolverá en el Notebook 02.

# COMMAND ----------

# MAGIC %md
# MAGIC **E2 · canales escritos de distintas formas.** Deberían ser 11 canales, pero hay más:

# COMMAND ----------

display(
    spark.table("bronze_pos_lineas")
         .groupBy("canal").count()
         .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **E1 · archivos reenviados** y **E3 · precios con coma decimal:**

# COMMAND ----------

pos_b = spark.table("bronze_pos_lineas")

display(
    pos_b.withColumn("archivo", F.element_at(F.split("_archivo_origen", "/"), -1))
         .filter(F.col("archivo").contains("reenvio"))
         .groupBy("archivo").count()
)

display(
    pos_b.filter(F.col("precio_unitario_q").contains(","))
         .select("ticket_id", "descripcion", "precio_unitario_q", "total_ticket_q")
         .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **E8 y E9 · el mismo pedido aparece varias veces en la app:**

# COMMAND ----------

display(
    spark.table("bronze_app_pedidos")
         .groupBy("order_id").agg(F.count("*").alias("eventos"),
                                  F.collect_list("status").alias("estados"))
         .filter("eventos > 1")
         .orderBy(F.desc("eventos"))
         .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **La estructura anidada de la app:** `items` es un arreglo de structs. En Silver se "explota"
# MAGIC (`explode`) para tener una fila por línea, igual que el POS.

# COMMAND ----------

spark.table("bronze_app_pedidos").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resultado
# MAGIC
# MAGIC Las 6 tablas quedan en **Catalog → workspace → mimcdonalds**. Desde ahí se puede ver, para cada
# MAGIC tabla, su descripción, su esquema, una muestra de datos y su **linaje** (pestaña *Lineage*): de
# MAGIC qué Volume salió.
# MAGIC
# MAGIC Siguiente paso: **Notebook 02 · Silver**, que lee **estas tablas** (no los archivos) y
# MAGIC corrige los errores E1–E18.
