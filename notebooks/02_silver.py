# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Capa SILVER — limpio, tipado y con un solo formato
# MAGIC
# MAGIC Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala
# MAGIC
# MAGIC Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis
# MAGIC
# MAGIC Lee las tablas `bronze_*` (nunca los archivos) y produce entidades limpias:
# MAGIC
# MAGIC | Tabla | Granularidad | Viene de |
# MAGIC |---|---|---|
# MAGIC | `silver_restaurantes` | 1 fila por restaurante | `bronze_restaurantes` |
# MAGIC | `silver_clientes` | 1 fila por cuenta | `bronze_clientes` |
# MAGIC | `silver_catalogo` | 1 fila por **versión** de recompensa | `bronze_catalogo` |
# MAGIC | `silver_transacciones` | 1 fila por ticket o pedido (**POS y app juntos**) | `bronze_pos_lineas` + `bronze_app_pedidos` |
# MAGIC | `silver_lineas` | 1 fila por línea de ticket o pedido | ídem |
# MAGIC | `silver_legado` | 1 fila por saldo del programa anterior | `bronze_legado` |
# MAGIC | `silver_cuarentena` | 1 fila por registro rechazado, con su motivo | todas |
# MAGIC
# MAGIC **Silver describe qué pasó; no aplica reglas del programa.** Aquí no se calculan puntos, no
# MAGIC se excluyen canales ni cuentas de Honduras: eso es Gold. Si mañana cambia una regla, este
# MAGIC notebook no se toca.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración

# COMMAND ----------

import unicodedata

from pyspark.sql import Column, Window
from pyspark.sql import functions as F

CATALOGO = "workspace"
ESQUEMA = "mimcdonalds"
ZONA_GT = "America/Guatemala"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funciones de limpieza
# MAGIC
# MAGIC Las transformaciones que se repiten viven en funciones con nombre. Así cada una se escribe
# MAGIC (y se prueba) **una sola vez**.
# MAGIC
# MAGIC - **`a_decimal` / `a_entero` usan `try_cast`, no `cast`.** En Serverless el modo ANSI está
# MAGIC   activo: un `cast('abc' AS DECIMAL)` **detiene todo el pipeline**. Con `try_cast` el valor malo
# MAGIC   queda nulo y el registro se manda a cuarentena.
# MAGIC - **Horas como `TIMESTAMP_NTZ`** ("hora de reloj en Guatemala", sin zona). La app manda UTC y se
# MAGIC   convierte con `convert_timezone`. Así el resultado **no depende** de la zona horaria de la
# MAGIC   sesión, que es una trampa clásica.
# MAGIC - **`a_cuarentena`** guarda el registro original completo como JSON, más el motivo. Nada se
# MAGIC   pierde en silencio.

# COMMAND ----------

def a_decimal(columna):
    """'22,00' o ' 22.00' -> 22.00 · algo inválido -> nulo (sin romper el pipeline)."""
    return F.expr(f"try_cast(replace(trim({columna}), ',', '.') AS DECIMAL(10,2))")


def a_entero(columna):
    return F.expr(f"try_cast(trim({columna}) AS INT)")


def texto(columna):
    """Quita espacios; un texto vacío se vuelve nulo."""
    limpio = F.trim(F.col(columna))
    return F.when(F.length(limpio) > 0, limpio)


def sin_tildes(columna):
    return F.translate(F.upper(F.trim(F.col(columna))), "ÁÉÍÓÚÜÑ", "AEIOUUN")


def utc_a_hora_gt(columna):
    """'2025-10-03T19:45:10Z' (UTC) -> 2025-10-03 13:45:10 (reloj de Guatemala)."""
    utc = F.to_timestamp_ntz(F.col(columna), F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))
    return F.convert_timezone(F.lit("UTC"), F.lit(ZONA_GT), utc)


cuarentena = []


def a_cuarentena(df, fuente, columna_id, motivo):
    motivo = motivo if isinstance(motivo, Column) else F.lit(motivo)
    return df.select(
        F.lit(fuente).alias("fuente"),
        F.col(columna_id).cast("string").alias("id_registro"),
        motivo.alias("motivo"),
        F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("registro_original"),
    )


def escribir_silver(df, tabla, descripcion):
    nombre = f"{CATALOGO}.{ESQUEMA}.{tabla}"
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre))
    spark.sql(f"COMMENT ON TABLE {nombre} IS '{descripcion}'")
    filas = spark.table(nombre).count()
    print(f"[silver] {tabla:<22} {filas:>7,} filas")
    return filas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Restaurantes · E16 (departamentos sin tilde)
# MAGIC
# MAGIC En vez de corregir caso por caso (`Quiche` → `Quiché`), se usa una **tabla de referencia**
# MAGIC con los 22 departamentos oficiales. Cada valor se compara sin tildes contra la lista y se
# MAGIC reemplaza por su forma oficial. Si mañana llega `PETEN` o `petén`, también se corrige.

# COMMAND ----------

DEPARTAMENTOS = ["Alta Verapaz", "Baja Verapaz", "Chimaltenango", "Chiquimula", "El Progreso",
                 "Escuintla", "Guatemala", "Huehuetenango", "Izabal", "Jalapa", "Jutiapa", "Petén",
                 "Quetzaltenango", "Quiché", "Retalhuleu", "Sacatepéquez", "San Marcos",
                 "Santa Rosa", "Sololá", "Suchitepéquez", "Totonicapán", "Zacapa"]


def _clave(texto_):
    sin = "".join(c for c in unicodedata.normalize("NFD", texto_) if unicodedata.category(c) != "Mn")
    return sin.upper()


ref_departamentos = spark.createDataFrame(
    [(_clave(d), d) for d in DEPARTAMENTOS], "clave STRING, departamento_oficial STRING")

b_rest = spark.table("bronze_restaurantes")
restaurantes = (
    b_rest.withColumn("clave", sin_tildes("departamento"))
          .join(F.broadcast(ref_departamentos), "clave", "left")
          .select(
              texto("restaurante_id").alias("restaurante_id"),
              texto("nombre").alias("nombre"),
              F.coalesce("departamento_oficial", texto("departamento")).alias("departamento"),
              texto("municipio").alias("municipio"),
              (F.upper(F.trim("tiene_automac")) == "SI").alias("tiene_automac"),
              (F.upper(F.trim("tiene_mcdelivery")) == "SI").alias("tiene_mcdelivery"),
              F.to_date(texto("fecha_apertura")).alias("fecha_apertura"),
              (F.col("departamento") != F.col("departamento_oficial")).alias("departamento_corregido"),
          )
          .dropDuplicates(["restaurante_id"])
)

escribir_silver(restaurantes, "silver_restaurantes",
                "Restaurantes con departamento oficial y banderas booleanas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clientes · E12, E13, E14
# MAGIC
# MAGIC - **E12 · correos:** `lower(trim())`. El correo es la llave de negocio (R18) y se usa para
# MAGIC   cruzar con el programa anterior; con mayúsculas o espacios, ese cruce fallaría.
# MAGIC - **E14 · teléfonos:** se dejan solo los dígitos y se lleva todo al formato internacional
# MAGIC   **E.164** (`+50255551234`). Un número de 8 dígitos no dice de qué país es, así que se usa el
# MAGIC   país de la cuenta.
# MAGIC - **E13 · misma persona con varias cuentas:** no se borra nada; se **marca**. Dos cuentas con el
# MAGIC   mismo nombre, apellido y teléfono son sospechosas, y la más antigua se considera la principal.
# MAGIC   Decidir qué hacer con ellas es negocio (Gold) o un modelo de fraude (Fase 4).
# MAGIC - **E15 · Honduras:** las cuentas se quedan con su país. Excluirlas del programa es la regla R20.

# COMMAND ----------

b_cli = spark.table("bronze_clientes")

digitos = F.regexp_replace(F.col("phone"), "[^0-9]", "")
prefijo = F.when(F.upper(F.trim("country")) == "HN", F.lit("+504")).otherwise(F.lit("+502"))

clientes = (
    b_cli.select(
        texto("customer_id").alias("customer_id"),
        F.lower(F.trim("email")).alias("correo"),
        texto("first_name").alias("nombre"),
        texto("last_name").alias("apellido"),
        F.when(F.length(digitos) == 8, F.concat(prefijo, digitos))
         .when(F.length(digitos) == 11, F.concat(F.lit("+"), digitos)).alias("telefono"),
        F.upper(F.trim("loyalty_code")).alias("codigo_lealtad"),
        F.upper(F.trim("country")).alias("pais"),
        utc_a_hora_gt("registered_at").alias("fecha_hora_registro_gt"),
    )
    .withColumn("fecha_registro_gt", F.to_date("fecha_hora_registro_gt"))
    .dropDuplicates(["customer_id"])
)

incompleto = (F.col("customer_id").isNull() | F.col("correo").isNull()
              | F.col("codigo_lealtad").isNull() | F.col("fecha_hora_registro_gt").isNull())
cuarentena.append(a_cuarentena(clientes.filter(incompleto), "crm", "customer_id", "campo_obligatorio_vacio"))
clientes = clientes.filter(~incompleto)

misma_persona = Window.partitionBy("nombre", "apellido", "telefono")
clientes = (
    clientes
    .withColumn("n_cuentas_misma_persona",
                F.when(F.col("telefono").isNotNull(), F.count("*").over(misma_persona)).otherwise(F.lit(1)))
    .withColumn("cuenta_principal_id",
                F.when(F.col("telefono").isNotNull(),
                       F.min_by("customer_id", "fecha_hora_registro_gt").over(misma_persona))
                 .otherwise(F.col("customer_id")))
    .withColumn("sospecha_duplicado", F.col("n_cuentas_misma_persona") > 1)
)

escribir_silver(clientes, "silver_clientes",
                "Cuentas con correo normalizado, telefono E.164, hora de registro en Guatemala y sospecha de duplicado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Catálogo de recompensas (versionado)
# MAGIC
# MAGIC Cada fila es una **versión**: el Big Tasty tiene dos (7,000 y 7,500 puntos). Es una
# MAGIC dimensión con historial, lo que en modelado de datos se llama **SCD tipo 2**. Gold la unirá
# MAGIC con cada canje usando la versión vigente **en la fecha del canje**.
# MAGIC
# MAGIC Para que eso funcione, las versiones de una misma recompensa **no pueden traslaparse**. Se
# MAGIC valida en la sección de comprobaciones.

# COMMAND ----------

catalogo = (
    spark.table("bronze_catalogo")
         .select(
             texto("recompensa_id").alias("recompensa_id"),
             texto("producto_id").alias("producto_id"),
             texto("producto").alias("producto"),
             a_entero("puntos").alias("puntos"),
             a_decimal("precio_referencia_q").alias("precio_referencia_q"),
             F.to_date(texto("vigente_desde")).alias("vigente_desde"),
             F.to_date(texto("vigente_hasta")).alias("vigente_hasta"),
         )
         .withColumn("es_oferta_bienvenida", F.col("puntos") == 0)
)

escribir_silver(catalogo, "silver_catalogo",
                "Catalogo de recompensas versionado (SCD tipo 2): una fila por version con su vigencia.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Transacciones del POS · E1, E2, E3, E4, E5, E6, E7
# MAGIC
# MAGIC | Error | Qué se hace |
# MAGIC |---|---|
# MAGIC | E2 canal mal escrito | Se quitan tildes, espacios y símbolos (`AUTO MAC` → `AUTOMAC`) y se aplica un mapa de **alias** (`drive` → `AUTOMAC`, `kiosko` → `KIOSCO`). Un canal que no se reconoce va a cuarentena |
# MAGIC | E3 coma decimal | `a_decimal` |
# MAGIC | E4 código con espacios o minúsculas | `upper(trim())` |
# MAGIC | E1 archivo reenviado | `dropDuplicates` por ticket + línea + estado |
# MAGIC | E7 ticket anulado | Si alguna fila del ticket dice `ANULADA`, el ticket queda `ANULADA` |
# MAGIC | E5 código inexistente | El ticket **se conserva** sin cliente y con `codigo_lealtad_valido = false`: la venta sí ocurrió |
# MAGIC | E6 total que no cuadra | El total se **recalcula desde las líneas** y se marca `total_cuadra = false` |

# COMMAND ----------

CANALES = ["MOSTRADOR", "AUTOMAC", "KIOSCO", "MCCAFE", "POSTRES", "MCDELIVERY", "PICKUP",
           "PEDIDOSYA", "UBEREATS", "CALLCENTER", "WHATSAPP"]
ALIAS_CANAL = {"DRIVE": "AUTOMAC", "KIOSKO": "KIOSCO", "CENTRODEPOSTRES": "POSTRES"}

clave_canal = F.regexp_replace(sin_tildes("canal"), "[^A-Z]", "")
mapa_alias = F.create_map(*[F.lit(x) for par in ALIAS_CANAL.items() for x in par])
canal_normalizado = F.coalesce(mapa_alias[clave_canal], clave_canal)

pos = (
    spark.table("bronze_pos_lineas")
         .select(
             texto("ticket_id").alias("transaccion_id"),
             F.upper(texto("restaurante_id")).alias("restaurante_id"),
             F.to_timestamp_ntz(F.col("fecha_hora_local"), F.lit("yyyy-MM-dd HH:mm:ss")).alias("fecha_hora_gt"),
             F.col("canal").alias("canal_original"),
             F.when(canal_normalizado.isin(CANALES), canal_normalizado).alias("canal"),
             F.col("codigo_lealtad").alias("codigo_original"),
             F.upper(texto("codigo_lealtad")).alias("codigo_lealtad"),
             a_entero("linea_num").alias("linea_num"),
             texto("producto_id").alias("producto_id"),
             texto("descripcion").alias("descripcion"),
             a_entero("cantidad").alias("cantidad"),
             a_decimal("precio_unitario_q").alias("precio_unitario_q"),
             F.col("precio_unitario_q").alias("precio_original"),
             F.upper(texto("tipo_linea")).alias("tipo_linea"),
             texto("recompensa_id").alias("recompensa_id"),
             a_decimal("total_ticket_q").alias("total_reportado_q"),
             F.upper(texto("estado")).alias("estado"),
         )
)

problema_pos = (F.when(F.col("canal").isNull(), "canal_desconocido")
                 .when(F.col("fecha_hora_gt").isNull(), "fecha_invalida")
                 .when(F.col("precio_unitario_q").isNull() | F.col("cantidad").isNull(), "precio_o_cantidad_invalido"))
cuarentena.append(a_cuarentena(pos.filter(problema_pos.isNotNull()), "pos", "transaccion_id", problema_pos))
pos = pos.filter(problema_pos.isNull())

# E1: el archivo reenviado trae filas idénticas
pos = pos.dropDuplicates(["transaccion_id", "linea_num", "estado"])

# E7: un ticket es ANULADA si alguna de sus filas lo dice
estado_ticket = (pos.groupBy("transaccion_id")
                    .agg(F.max(F.col("estado") == "ANULADA").alias("anulado")))

lineas_pos = (pos.filter(F.col("estado") == "COMPLETADA")
                 .select("transaccion_id", "linea_num", "producto_id", "descripcion", "cantidad",
                         "precio_unitario_q", "tipo_linea", "recompensa_id"))

cabeceras_pos = (
    pos.groupBy("transaccion_id")
       .agg(F.max("restaurante_id").alias("restaurante_id"),
            F.max("fecha_hora_gt").alias("fecha_hora_gt"),
            F.max("canal").alias("canal"),
            F.max("codigo_lealtad").alias("codigo_lealtad"),
            F.max("total_reportado_q").alias("total_reportado_q"),
            F.max(F.col("canal_original") != F.col("canal")).alias("canal_corregido"),
            F.max(F.coalesce(F.col("codigo_original") != F.col("codigo_lealtad"), F.lit(False))).alias("codigo_corregido"),
            F.max(F.col("precio_original").contains(",")).alias("precio_con_coma"))
       .join(estado_ticket, "transaccion_id")
       .withColumn("estado", F.when(F.col("anulado"), "ANULADA").otherwise("COMPLETADA"))
       .drop("anulado")
)

# E5: el POS solo conoce el código; el cliente se resuelve contra silver_clientes
codigos = spark.table("silver_clientes").select("codigo_lealtad", "customer_id")
cabeceras_pos = (
    cabeceras_pos.join(codigos, "codigo_lealtad", "left")
                 .withColumn("codigo_lealtad_valido",
                             F.when(F.col("codigo_lealtad").isNotNull(), F.col("customer_id").isNotNull()))
                 .withColumn("origen", F.lit("POS"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pedidos de la app · E8, E9, E11
# MAGIC
# MAGIC 1. **E11 · campos faltantes → cuarentena.** El contrato de la app promete `order_id`,
# MAGIC    `customer_id`, `store_id` y `created_at` siempre. Si falta uno, el evento está corrupto.
# MAGIC 2. **E8 · eventos duplicados:** la app entrega "al menos una vez", así que un evento puede
# MAGIC    llegar repetido. Se quitan los idénticos.
# MAGIC 3. **E9 · cancelaciones:** de cada pedido se queda el **último** evento (por `updated_at`), con
# MAGIC    una *window function*. Si es `CANCELLED`, el pedido queda `ANULADA`.
# MAGIC 4. **Aplanar `items`:** `posexplode` convierte el arreglo en una fila por línea, con su posición.
# MAGIC 5. **Unificar con el POS:** mismos nombres de columna, mismos valores (`PRODUCT` →
# MAGIC    `PRODUCTO`), `store_id = 12` → `R012`, y hora UTC → Guatemala.

# COMMAND ----------

b_app = spark.table("bronze_app_pedidos")

LLAVES_APP = ["order_id", "customer_id", "store_id", "created_at"]
faltantes = F.concat_ws(",", *[F.when(F.col(c).isNull(), F.lit(c)) for c in LLAVES_APP])

eventos_malos = b_app.filter(faltantes != "").dropDuplicates(["order_id"])
cuarentena.append(a_cuarentena(eventos_malos, "app", "order_id",
                               F.concat(F.lit("campo_faltante:"), faltantes)))

ultimo_evento = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
app = (
    b_app.filter(faltantes == "")
         .dropDuplicates(["order_id", "status", "updated_at"])                          # E8
         .withColumn("n", F.row_number().over(ultimo_evento))
         .filter("n = 1")                                                               # E9
         .drop("n")
)

cabeceras_app = app.select(
    F.col("order_id").alias("transaccion_id"),
    F.lit("APP").alias("origen"),
    F.concat(F.lit("R"), F.lpad(F.trim("store_id"), 3, "0")).alias("restaurante_id"),
    utc_a_hora_gt("created_at").alias("fecha_hora_gt"),
    F.upper(F.trim("channel")).alias("canal"),
    F.col("customer_id"),
    F.lit(None).cast("string").alias("codigo_lealtad"),
    F.lit(None).cast("boolean").alias("codigo_lealtad_valido"),
    F.when(F.col("status") == "CANCELLED", "ANULADA").otherwise("COMPLETADA").alias("estado"),
    a_decimal("total").alias("total_reportado_q"),
    F.lit(False).alias("canal_corregido"),
    F.lit(False).alias("codigo_corregido"),
    F.lit(False).alias("precio_con_coma"),
)

TIPO_LINEA_APP = {"PRODUCT": "PRODUCTO", "REWARD": "CANJE", "DONATION": "DONACION"}
mapa_tipo = F.create_map(*[F.lit(x) for par in TIPO_LINEA_APP.items() for x in par])

lineas_app = (
    app.select("order_id", F.posexplode("items").alias("pos", "item"))
       .select(
           F.col("order_id").alias("transaccion_id"),
           (F.col("pos") + 1).alias("linea_num"),
           F.col("item.sku").alias("producto_id"),
           F.col("item.name").alias("descripcion"),
           a_entero("item.qty").alias("cantidad"),
           a_decimal("item.unit_price").alias("precio_unitario_q"),
           mapa_tipo[F.col("item.type")].alias("tipo_linea"),
           F.col("item.reward_id").alias("recompensa_id"),
       )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Unificar POS + app → `silver_transacciones` y `silver_lineas`
# MAGIC
# MAGIC Desde aquí **ya no importa de dónde vino la compra**: Gold aplicará las mismas reglas a todas.
# MAGIC Los montos se calculan **desde las líneas** (E6), separando productos de donaciones (R4),
# MAGIC y se guarda la **franja horaria** porque el mínimo para canjear en McDelivery depende de ella
# MAGIC (R15).

# COMMAND ----------

lineas = (
    lineas_pos.unionByName(lineas_app)
              .withColumn("subtotal_q", (F.col("cantidad") * F.col("precio_unitario_q")).cast("decimal(12,2)"))
)

montos = lineas.groupBy("transaccion_id").agg(
    F.sum(F.when(F.col("tipo_linea") == "PRODUCTO", F.col("subtotal_q")).otherwise(0)).cast("decimal(12,2)").alias("monto_productos_q"),
    F.sum(F.when(F.col("tipo_linea") == "DONACION", F.col("subtotal_q")).otherwise(0)).cast("decimal(12,2)").alias("monto_donacion_q"),
    F.sum(F.when(F.col("tipo_linea") == "CANJE", 1).otherwise(0)).cast("int").alias("n_canjes"),
)

transacciones = (
    cabeceras_pos.unionByName(cabeceras_app)
                 .join(montos, "transaccion_id")
                 .withColumn("total_calculado_q", (F.col("monto_productos_q") + F.col("monto_donacion_q")).cast("decimal(12,2)"))
                 .withColumn("total_cuadra", F.col("total_reportado_q") == F.col("total_calculado_q"))
                 .withColumn("fecha_gt", F.to_date("fecha_hora_gt"))
                 .withColumn("franja", F.when(F.hour("fecha_hora_gt") < 11, "DESAYUNO").otherwise("ALMUERZO_CENA"))
                 .select("transaccion_id", "origen", "restaurante_id", "canal", "fecha_hora_gt", "fecha_gt",
                         "franja", "customer_id", "codigo_lealtad", "codigo_lealtad_valido", "estado",
                         "monto_productos_q", "monto_donacion_q", "total_calculado_q", "total_reportado_q",
                         "total_cuadra", "n_canjes", "canal_corregido", "codigo_corregido", "precio_con_coma")
)

escribir_silver(transacciones, "silver_transacciones",
                "Tickets del POS y pedidos de la app unificados. Hora de Guatemala, montos recalculados desde las lineas.")
escribir_silver(lineas, "silver_lineas",
                "Lineas de tickets y pedidos, sin duplicados, con tipos correctos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Programa anterior · E17, E18
# MAGIC
# MAGIC - La fecha viene como `dd/mm/aaaa` y se convierte con su formato explícito.
# MAGIC - El correo se normaliza igual que en clientes, para poder **cruzarlos** (resolver identidad es
# MAGIC   trabajo de Silver).
# MAGIC - **E17:** si el correo no existe en el CRM, el saldo queda `PENDIENTE_REGISTRO`; no se pierde.
# MAGIC - **E18:** un saldo negativo o ilegible va a cuarentena.

# COMMAND ----------

legado = (
    spark.table("bronze_legado")
         .select(F.lower(F.trim("email")).alias("correo"),
                 a_entero("saldo_puntos").alias("saldo_puntos_legado"),
                 F.to_date(texto("ultima_compra"), "dd/MM/yyyy").alias("ultima_compra"))
         .dropDuplicates(["correo"])
)
saldo_invalido = F.col("saldo_puntos_legado").isNull() | (F.col("saldo_puntos_legado") < 0)
cuarentena.append(a_cuarentena(legado.filter(saldo_invalido), "legado", "correo", "saldo_legado_invalido"))

cuentas = spark.table("silver_clientes").select("correo", "customer_id")
legado = (
    legado.filter(~saldo_invalido)
          .join(cuentas, "correo", "left")
          .withColumn("estado_migracion",
                      F.when(F.col("customer_id").isNull(), "PENDIENTE_REGISTRO").otherwise("REGISTRADO"))
)

escribir_silver(legado, "silver_legado",
                "Saldos del programa Puntos McDelivery cruzados con las cuentas actuales por correo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cuarentena
# MAGIC
# MAGIC Todos los rechazos de todas las fuentes en **una sola tabla**, con el registro original
# MAGIC completo. Un analista puede preguntar "¿cuántos datos perdimos y por qué?" con un `GROUP BY`.

# COMMAND ----------

todos = cuarentena[0]
for parte in cuarentena[1:]:
    todos = todos.unionByName(parte)
todos = todos.withColumn("_fecha_proceso", F.current_timestamp())

escribir_silver(todos, "silver_cuarentena",
                "Registros rechazados por Silver con su motivo y el registro original en JSON.")

display(spark.table("silver_cuarentena").groupBy("fuente", "motivo").count().orderBy("fuente"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Comprobaciones
# MAGIC
# MAGIC ### 11.1 Expectativas de calidad
# MAGIC
# MAGIC Son afirmaciones que **siempre** deben cumplirse en Silver, sin importar los datos. Si una
# MAGIC falla, el notebook se detiene: es mejor no publicar Silver que publicar un Silver sucio. En
# MAGIC producción esto se haría con las *expectations* de Lakeflow Declarative Pipelines.

# COMMAND ----------

t = spark.table("silver_transacciones")
l = spark.table("silver_lineas")
c = spark.table("silver_clientes")
r = spark.table("silver_restaurantes")
cat = spark.table("silver_catalogo")

version_anterior = Window.partitionBy("recompensa_id").orderBy("vigente_desde")
traslapes = (cat.withColumn("hasta_anterior", F.lag("vigente_hasta").over(version_anterior))
                .filter(F.col("hasta_anterior").isNotNull()
                        & (F.col("hasta_anterior") >= F.col("vigente_desde")))
                .count())

expectativas = {
    "transacciones duplicadas": t.count() - t.select("transaccion_id").distinct().count(),
    "lineas duplicadas": l.count() - l.select("transaccion_id", "linea_num").distinct().count(),
    "canales fuera de la lista oficial": t.filter(~F.col("canal").isin(CANALES)).count(),
    "transacciones sin fecha": t.filter(F.col("fecha_hora_gt").isNull()).count(),
    "lineas sin precio": l.filter(F.col("precio_unitario_q").isNull()).count(),
    "lineas con tipo desconocido": l.filter(F.col("tipo_linea").isNull()).count(),
    "correos sin normalizar": c.filter(F.col("correo") != F.lower(F.trim("correo"))).count(),
    "telefonos fuera de E.164": c.filter(F.col("telefono").isNotNull() & ~F.col("telefono").rlike(r"^\+50[24][0-9]{8}$")).count(),
    "departamentos no oficiales": r.filter(~F.col("departamento").isin(DEPARTAMENTOS)).count(),
    "transacciones de restaurantes inexistentes": t.join(r, "restaurante_id", "left_anti").count(),
    "ventas antes de la apertura del restaurante": t.join(r, "restaurante_id").filter(F.col("fecha_gt") < F.col("fecha_apertura")).count(),
    "versiones del catalogo traslapadas": traslapes,
}
for nombre, valor in expectativas.items():
    print(f"{'OK ' if valor == 0 else 'FALLA'} {nombre:<45} {valor}")

assert all(v == 0 for v in expectativas.values()), "Silver no cumple sus expectativas de calidad"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Reporte de calidad: ¿qué problemas detectó Silver?
# MAGIC
# MAGIC Cada error E1–E18 deja una huella que Silver puede contar. Los conteos se guardan en
# MAGIC `silver_calidad_detecciones`: es el **reporte de calidad de datos** de esta corrida, y la
# MAGIC Fase 5 (`05_evaluacion`) lo compara contra lo que el generador inyectó.
# MAGIC
# MAGIC ¿Por qué no se compara aquí? Porque el pipeline **no debe leer la hoja de respuestas**: en
# MAGIC producción no existe. La primera versión de este notebook comparaba contra números fijos de
# MAGIC la semilla 42, y esa prueba se rompió en cuanto se regeneraron los datos.
# MAGIC
# MAGIC E10 no aparece aquí: un canje bajo el mínimo es una violación de regla (R15) y lo detecta Gold.

# COMMAND ----------

b_pos = spark.table("bronze_pos_lineas")
b_cli = spark.table("bronze_clientes")
q = spark.table("silver_cuarentena")
leg = spark.table("silver_legado")
pos_t = t.filter(F.col("origen") == "POS")
app_t = t.filter(F.col("origen") == "APP")

detectado = {
    "E1  filas de archivos reenviados": b_pos.count() - b_pos.dropDuplicates(
        [c_ for c_ in b_pos.columns if not c_.startswith("_")]).count(),
    "E2  tickets con canal mal escrito": pos_t.filter("canal_corregido").count(),
    "E3  tickets con coma decimal": pos_t.filter("precio_con_coma").count(),
    "E4  tickets con codigo mal escrito": pos_t.filter("codigo_corregido").count(),
    "E5  tickets con codigo inexistente": pos_t.filter(F.col("codigo_lealtad_valido") == False).count(),  # noqa: E712
    "E6  tickets cuyo total no cuadra": t.filter(~F.col("total_cuadra")).count(),
    "E7  tickets del POS anulados": pos_t.filter(F.col("estado") == "ANULADA").count(),
    "E8  pedidos con eventos duplicados": b_app.filter(faltantes == "")
                                          .groupBy("order_id", "status", "updated_at").count()
                                          .filter("count > 1").select("order_id").distinct().count(),
    "E9  pedidos de la app cancelados": app_t.filter(F.col("estado") == "ANULADA").count(),
    "E11 pedidos de la app en cuarentena": q.filter(F.col("fuente") == "app").count(),
    "E12 correos corregidos": b_cli.filter(F.col("email") != F.lower(F.trim("email"))).count(),
    "E13 cuentas duplicadas (no principales)": c.filter(F.col("sospecha_duplicado") & (F.col("customer_id") != F.col("cuenta_principal_id"))).count(),
    "E14 telefonos reformateados": b_cli.join(c, "customer_id").filter(F.col("phone") != F.col("telefono")).count(),
    "E15 cuentas de Honduras": c.filter(F.col("pais") == "HN").count(),
    "E16 departamentos corregidos": r.filter("departamento_corregido").count(),
    "E17 saldos pendientes de registro": leg.filter(F.col("estado_migracion") == "PENDIENTE_REGISTRO").count(),
    "E18 saldos negativos en cuarentena": q.filter(F.col("fuente") == "legado").count(),
}

for error, n in detectado.items():
    print(f"{error:<42} {n:>7,}")

calidad = spark.createDataFrame(
    [(e.split()[0], " ".join(e.split()[1:]), n) for e, n in detectado.items()],
    "error STRING, descripcion STRING, detectado LONG",
).withColumn("_fecha_proceso", F.current_timestamp())

escribir_silver(calidad, "silver_calidad_detecciones",
                "Reporte de calidad: cuantos registros con cada tipo de error detecto Silver en esta corrida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Resultado
# MAGIC
# MAGIC Ocho tablas `silver_*` en `workspace.mimcdonalds`. Como cada una se construyó leyendo tablas
# MAGIC con `spark.table(...)`, la pestaña **Lineage** de `silver_transacciones` ya muestra que viene
# MAGIC de `bronze_pos_lineas` y `bronze_app_pedidos`.
# MAGIC
# MAGIC Siguiente paso: **Notebook 03 · Gold**, donde por fin se aplican las reglas del programa y se
# MAGIC calculan los puntos.
