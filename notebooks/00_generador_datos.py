# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generador de datos sintéticos — MiMcDonald's Guatemala
# MAGIC
# MAGIC Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala
# MAGIC
# MAGIC Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis
# MAGIC
# MAGIC El cliente no nos entregó datos, así que este notebook **simula los 6 sistemas origen** del
# MAGIC programa MiMcDonald's (ver `docs/02_entendimiento_datos.md`) y deja sus archivos en el
# MAGIC Volume `landing`, como si los sistemas los hubieran enviado.
# MAGIC
# MAGIC Los datos:
# MAGIC 1. **respetan las reglas** R1–R22 de la Fase 1, y
# MAGIC 2. **traen los errores E1–E18 inyectados a propósito**, para que Silver tenga trabajo real.
# MAGIC
# MAGIC Además, el generador lleva **su propia contabilidad de puntos** y la guarda en otro Volume
# MAGIC (`control`). Es la **hoja de respuestas**: el pipeline **nunca la lee**, y en la Fase 5 se
# MAGIC compara contra lo que calcule Gold.
# MAGIC
# MAGIC Corre igual en Databricks y en una computadora local: si no encuentra `spark`, escribe en
# MAGIC `./_datos_locales/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros y reglas del programa
# MAGIC
# MAGIC Todas las reglas de la Fase 1 están **en un solo lugar** y con nombre. Si una regla cambia,
# MAGIC se cambia aquí y en ningún otro lado. Es justo lo que le faltó al sistema del cliente.

# COMMAND ----------

import csv
import json
import math
import os
import random
import shutil
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

CATALOGO = "workspace"
ESQUEMA = "mimcdonalds"

SEMILLA = 42                       # misma semilla = mismos datos en cada corrida
N_CLIENTES = 5_000
FECHA_INICIO = date(2025, 8, 27)   # lanzamiento de MiMcDonald's
FECHA_FIN = date(2026, 9, 20)      # > 365 días, para que haya vencimientos reales

# --- Reglas del programa (docs/01_entendimiento_negocio.md) ---
PUNTOS_POR_QUETZAL = 10            # R1
TOPE_DIARIO_ACUMULACION = 1_000    # R8
TOPE_DIARIO_CANJE = 15_000         # R12
MAX_OFERTAS_POR_PEDIDO = 3         # R13
DIAS_VIGENCIA = 365                # R10
PUNTOS_BIENVENIDA = 1_000          # R17
FACTOR_MIGRACION = 10              # H1 (supuesto: se conserva el valor en quetzales)
MINIMO_CANJE_DELIVERY_CENT = {"DESAYUNO": 5_000, "ALMUERZO_CENA": 6_000}   # R15
DESFASE_UTC_HORAS = 6              # Guatemala = UTC-6, sin horario de verano

CANALES_POS_ACUMULAN = ["MOSTRADOR", "AUTOMAC", "KIOSCO", "MCCAFE", "POSTRES"]   # R6
CANALES_APP = ["MCDELIVERY", "PICKUP"]                                           # R6
CANALES_EXCLUIDOS = ["PEDIDOSYA", "UBEREATS", "CALLCENTER", "WHATSAPP"]          # R7
CANALES_CON_CANJE = {"MOSTRADOR", "AUTOMAC", "KIOSCO", "MCDELIVERY", "PICKUP"}

rng = random.Random(SEMILLA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Entorno: ¿Databricks o local?
# MAGIC
# MAGIC En Databricks se crean el schema y **dos Volumes**:
# MAGIC
# MAGIC | Volume | Qué guarda | ¿Lo lee el pipeline? |
# MAGIC |---|---|---|
# MAGIC | `landing` | Los archivos de los 6 sistemas origen | Sí (Bronze) |
# MAGIC | `control` | La hoja de respuestas del generador | **No**, solo la evaluación (Fase 5) |
# MAGIC
# MAGIC Antes de escribir se **borra lo anterior**: así el notebook es *idempotente*, y correrlo dos
# MAGIC veces da exactamente los mismos archivos.

# COMMAND ----------

try:
    spark  # noqa: F821  (existe solo dentro de Databricks)
    EN_DATABRICKS = True
except NameError:
    EN_DATABRICKS = False

if EN_DATABRICKS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.landing")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.control")
    RUTA_LANDING = f"/Volumes/{CATALOGO}/{ESQUEMA}/landing"
    RUTA_CONTROL = f"/Volumes/{CATALOGO}/{ESQUEMA}/control"
else:
    RUTA_LANDING = os.path.join(os.getcwd(), "_datos_locales", "landing")
    RUTA_CONTROL = os.path.join(os.getcwd(), "_datos_locales", "control")


def limpiar_carpeta(ruta):
    """Borra el contenido de la carpeta (no la carpeta: la raíz de un Volume no se puede borrar)."""
    os.makedirs(ruta, exist_ok=True)
    for nombre in os.listdir(ruta):
        completa = os.path.join(ruta, nombre)
        if os.path.isdir(completa):
            shutil.rmtree(completa)
        else:
            os.remove(completa)


limpiar_carpeta(RUTA_LANDING)
limpiar_carpeta(RUTA_CONTROL)
print(f"[entorno] {'Databricks' if EN_DATABRICKS else 'local'}")
print(f"[entorno] landing: {RUTA_LANDING}")
print(f"[entorno] control: {RUTA_CONTROL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Datos maestros: restaurantes, menú y catálogo de recompensas
# MAGIC
# MAGIC - **30 restaurantes** de los 126 reales, con más peso en el departamento de Guatemala.
# MAGIC   Los nombres son ilustrativos. `R009` abre **durante** el periodo (15-mar-2026), así que no
# MAGIC   puede tener ventas antes de esa fecha.
# MAGIC - **Precios en centavos** (enteros): con decimales binarios `0.1 + 0.2 ≠ 0.3`, y en un
# MAGIC   sistema de puntos un centavo de error termina en un reclamo. Algunos precios terminan en
# MAGIC   `.25` o `.75` para poner a prueba el redondeo hacia arriba (R2).
# MAGIC - El **catálogo está versionado** (R16): el Big Tasty sube de 7,000 a 7,500 puntos en marzo, el
# MAGIC   McMuffin entra en enero y el café sale del catálogo en junio. Gold tendrá que usar la versión
# MAGIC   vigente **en la fecha del canje**.

# COMMAND ----------

# id, nombre, departamento, municipio, automac, mcdelivery, fecha_apertura
RESTAURANTES = [
    ("R001", "McDonald's Roosevelt", "Guatemala", "Guatemala", True, True, "1985-06-01"),
    ("R002", "McDonald's Reforma", "Guatemala", "Guatemala", False, True, "1974-10-01"),
    ("R003", "McDonald's Oakland Mall", "Guatemala", "Guatemala", False, True, "2013-11-15"),
    ("R004", "McDonald's Miraflores", "Guatemala", "Guatemala", False, True, "2005-03-10"),
    ("R005", "McDonald's Pradera Concepción", "Guatemala", "Santa Catarina Pinula", True, True, "2016-05-20"),
    ("R006", "McDonald's Naranjo Mall", "Guatemala", "Mixco", False, True, "2011-08-01"),
    ("R007", "McDonald's San Cristóbal", "Guatemala", "Mixco", True, True, "2002-04-12"),
    ("R008", "McDonald's Villa Nueva Centro", "Guatemala", "Villa Nueva", True, True, "2008-09-09"),
    ("R009", "McDonald's El Frutal", "Guatemala", "Villa Nueva", False, False, "2026-03-15"),
    ("R010", "McDonald's Petapa", "Guatemala", "San Miguel Petapa", True, True, "2014-07-07"),
    ("R011", "McDonald's Carretera a El Salvador", "Guatemala", "Santa Catarina Pinula", True, True, "2010-02-02"),
    ("R012", "McDonald's Majadas", "Guatemala", "Guatemala", True, True, "2012-12-12"),
    ("R013", "McDonald's Portales", "Guatemala", "Guatemala", False, True, "2010-10-10"),
    ("R014", "McDonald's Zona 10", "Guatemala", "Guatemala", True, True, "1990-05-05"),
    ("R015", "McDonald's Cayalá", "Guatemala", "Guatemala", False, True, "2015-11-20"),
    ("R016", "McDonald's Chinautla", "Guatemala", "Chinautla", True, False, "2019-06-15"),
    ("R017", "McDonald's Antigua", "Sacatepéquez", "Antigua Guatemala", False, False, "1998-03-03"),
    ("R018", "McDonald's Xela Pradera", "Quetzaltenango", "Quetzaltenango", True, True, "2004-08-08"),
    ("R019", "McDonald's Escuintla", "Escuintla", "Escuintla", True, True, "2007-01-20"),
    ("R020", "McDonald's Santa Cruz del Quiché", "Quiché", "Santa Cruz del Quiché", True, False, "2021-07-01"),
    ("R021", "McDonald's Cobán", "Alta Verapaz", "Cobán", True, True, "2012-04-04"),
    ("R022", "McDonald's Santa Elena", "Petén", "Flores", True, False, "2016-09-09"),
    ("R023", "McDonald's Puerto Barrios", "Izabal", "Puerto Barrios", True, False, "2017-02-14"),
    ("R024", "McDonald's Huehuetenango", "Huehuetenango", "Huehuetenango", True, False, "2018-10-10"),
    ("R025", "McDonald's Chimaltenango", "Chimaltenango", "Chimaltenango", True, True, "2011-11-11"),
    ("R026", "McDonald's Retalhuleu", "Retalhuleu", "Retalhuleu", True, False, "2013-03-03"),
    ("R027", "McDonald's Mazatenango", "Suchitepéquez", "Mazatenango", True, False, "2014-04-14"),
    ("R028", "McDonald's Zacapa", "Zacapa", "Zacapa", True, False, "2015-05-15"),
    ("R029", "McDonald's Jutiapa", "Jutiapa", "Jutiapa", True, False, "2019-09-19"),
    ("R030", "McDonald's San Marcos", "San Marcos", "San Marcos", True, False, "2020-01-20"),
]
REST = {r[0]: {"id": r[0], "automac": r[4], "mcdelivery": r[5],
               "apertura": date.fromisoformat(r[6]),
               "peso": 3 if r[2] == "Guatemala" else 1} for r in RESTAURANTES}

# producto_id: (descripción, precio en centavos, categoría, franja)
MENU = {
    "P-COMBO-BIGMAC": ("Combo Big Mac", 6200, "COMIDA", "ALMUERZO_CENA"),
    "P-COMBO-CUARTO": ("Combo Cuarto de Libra con queso", 6600, "COMIDA", "ALMUERZO_CENA"),
    "P-COMBO-MCPOLLO": ("Combo McPollo", 4900, "COMIDA", "ALMUERZO_CENA"),
    "P-BIGMAC": ("Big Mac", 4200, "COMIDA", "ALMUERZO_CENA"),
    "P-BIGTASTY": ("Big Tasty", 5500, "COMIDA", "ALMUERZO_CENA"),
    "P-MCPOLLO": ("McPollo", 3200, "COMIDA", "ALMUERZO_CENA"),
    "P-QUESOB": ("Quesoburguesa", 1800, "COMIDA", "ALMUERZO_CENA"),
    "P-HAMB": ("Hamburguesa", 1550, "COMIDA", "ALMUERZO_CENA"),
    "P-NUGGETS6": ("McNuggets 6 piezas", 3000, "COMIDA", "ALMUERZO_CENA"),
    "P-NUGGETS10": ("McNuggets 10 piezas", 4200, "COMIDA", "ALMUERZO_CENA"),
    "P-CAJITA": ("Cajita Feliz", 3800, "COMIDA", "ALMUERZO_CENA"),
    "P-PAPAS-M": ("Papas medianas", 1800, "COMIDA", "TODO"),
    "P-PAPAS-G": ("Papas grandes", 2200, "COMIDA", "TODO"),
    "P-GASEOSA-M": ("Gaseosa mediana", 1400, "COMIDA", "TODO"),
    "P-MCMUFFIN": ("McMuffin con huevo y salchicha", 2800, "COMIDA", "DESAYUNO"),
    "P-DESAYUNO-DLX": ("Desayuno Deluxe", 4500, "COMIDA", "DESAYUNO"),
    "P-HOTCAKES": ("Hotcakes", 2600, "COMIDA", "DESAYUNO"),
    "P-CAFE-AMER": ("Café americano", 1275, "MCCAFE", "TODO"),
    "P-CAPUCHINO": ("Capuchino", 2250, "MCCAFE", "TODO"),
    "P-PASTEL-MANZ": ("Pastel de manzana", 1050, "MCCAFE", "TODO"),
    "P-CONO": ("Cono de vainilla", 550, "POSTRE", "TODO"),
    "P-MCFLURRY": ("McFlurry", 2200, "POSTRE", "TODO"),
    "P-SUNDAE": ("Sundae", 1525, "POSTRE", "TODO"),
}
PESO_PRODUCTO = {"P-COMBO-BIGMAC": 8, "P-COMBO-CUARTO": 6, "P-COMBO-MCPOLLO": 6, "P-CAJITA": 4,
                 "P-MCMUFFIN": 6, "P-DESAYUNO-DLX": 4, "P-HOTCAKES": 4}
DONACION = ("D-RONALD", "Donación Casa Ronald McDonald")

# recompensa_id, producto_id, puntos, vigente_desde, vigente_hasta (None = sigue vigente)
CATALOGO_RECOMPENSAS = [
    ("RW-BIENVENIDA", "P-QUESOB", 0, "2025-08-27", None),      # oferta única de registro (R17)
    ("RW-PAPAS-M", "P-PAPAS-M", 3000, "2025-08-27", None),
    ("RW-NUGGETS6", "P-NUGGETS6", 3000, "2025-08-27", None),
    ("RW-CAFE", "P-CAFE-AMER", 3000, "2025-08-27", "2026-06-30"),   # sale del catálogo
    ("RW-QUESOB", "P-QUESOB", 4000, "2025-08-27", None),
    ("RW-MCFLURRY", "P-MCFLURRY", 4500, "2025-08-27", None),
    ("RW-MCMUFFIN", "P-MCMUFFIN", 5000, "2026-01-15", None),        # entra al catálogo
    ("RW-MCPOLLO", "P-MCPOLLO", 5500, "2025-08-27", None),
    ("RW-BIGMAC", "P-BIGMAC", 6500, "2025-08-27", None),
    ("RW-BIGTASTY", "P-BIGTASTY", 7000, "2025-08-27", "2026-02-28"),  # cambio de precio
    ("RW-BIGTASTY", "P-BIGTASTY", 7500, "2026-03-01", None),
    ("RW-DESAYUNO-DLX", "P-DESAYUNO-DLX", 7500, "2025-08-27", None),
]


def recompensas_vigentes(dia, franja):
    """Versión del catálogo vigente ese día, sin la bienvenida y respetando la franja horaria."""
    salida = []
    for rid, pid, puntos, desde, hasta in CATALOGO_RECOMPENSAS:
        if rid == "RW-BIENVENIDA":
            continue
        if not (date.fromisoformat(desde) <= dia <= date.fromisoformat(hasta or "9999-12-31")):
            continue
        if MENU[pid][3] not in ("TODO", franja):
            continue
        salida.append((rid, pid, puntos))
    return salida

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funciones de apoyo

# COMMAND ----------

NOMBRES = ["José", "María", "Juan", "Ana", "Luis", "Carmen", "Carlos", "Sofía", "Jorge", "Lucía",
           "Mario", "Andrea", "Pedro", "Gabriela", "Fernando", "Valeria", "Ricardo", "Daniela",
           "Miguel", "Alejandra", "Javier", "Paola", "Óscar", "Fernanda", "Kevin", "Mónica", "Edgar",
           "Karla", "Byron", "Ingrid", "Walter", "Heidy", "Erick", "Claudia", "Rosa", "Julio"]
APELLIDOS = ["López", "García", "Pérez", "Hernández", "González", "Rodríguez", "Morales",
             "Martínez", "Castillo", "Ramírez", "Méndez", "Cruz", "Reyes", "Juárez", "Orellana",
             "Barrios", "Chávez", "Monterroso", "Cifuentes", "Ajú", "Tzul", "Xicará", "Coy", "Pop",
             "Caal", "Estrada", "Girón", "Aguilar", "Recinos", "Samayoa"]
DOMINIOS = ["gmail.com"] * 12 + ["hotmail.com"] * 4 + ["yahoo.com", "outlook.com", "icloud.com"]
ALFABETO_CODIGO = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"   # sin O/0 ni I/1, que se confunden en caja

HORAS_DESAYUNO = {6: 3, 7: 6, 8: 7, 9: 6, 10: 4}
HORAS_ALMUERZO_CENA = {11: 5, 12: 12, 13: 14, 14: 9, 15: 5, 16: 5, 17: 7, 18: 10, 19: 12,
                       20: 10, 21: 6, 22: 3}
PESO_CANAL = {"MOSTRADOR": 25, "AUTOMAC": 25, "KIOSCO": 12, "MCCAFE": 5, "POSTRES": 5,
              "MCDELIVERY": 12, "PICKUP": 6, "PEDIDOSYA": 4, "UBEREATS": 2, "CALLCENTER": 1,
              "WHATSAPP": 1}


def quitar_tildes(texto):
    return "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")


def quetzales(centavos, coma=False):
    texto = f"{centavos / 100:.2f}"
    return texto.replace(".", ",") if coma else texto


def elegir(pesos):
    """Elige una llave de un dict {opción: peso}."""
    opciones = list(pesos)
    return rng.choices(opciones, weights=[pesos[o] for o in opciones])[0]


def poisson(lam):
    limite, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limite:
            return k
        k += 1


def nuevo_codigo(usados):
    while True:
        codigo = "".join(rng.choices(ALFABETO_CODIGO, k=8))
        if codigo not in usados:
            usados.add(codigo)
            return codigo


def momento_aleatorio(dia, p_desayuno):
    franja = "DESAYUNO" if rng.random() < p_desayuno else "ALMUERZO_CENA"
    hora = elegir(HORAS_DESAYUNO if franja == "DESAYUNO" else HORAS_ALMUERZO_CENA)
    return datetime(dia.year, dia.month, dia.day, hora, rng.randrange(60), rng.randrange(60)), franja


def restaurante_para(dia, preferido):
    abiertos = [r for r in REST.values() if r["apertura"] <= dia]
    if preferido["apertura"] <= dia and rng.random() < 0.8:
        return preferido
    return rng.choices(abiertos, weights=[r["peso"] for r in abiertos])[0]


def canal_para(restaurante, pesos_cliente, solo_app=False):
    pesos = {c: p for c, p in pesos_cliente.items()
             if not (c == "AUTOMAC" and not restaurante["automac"])
             and not (c == "MCDELIVERY" and not restaurante["mcdelivery"])
             and not (solo_app and c not in CANALES_APP)}
    return elegir(pesos) if pesos else "MOSTRADOR"


def armar_lineas(canal, franja):
    if canal == "MCCAFE":
        pool, n = [p for p, v in MENU.items() if v[2] == "MCCAFE"], rng.randint(1, 2)
    elif canal == "POSTRES":
        pool, n = [p for p, v in MENU.items() if v[2] == "POSTRE"], rng.randint(1, 2)
    else:
        pool = [p for p, v in MENU.items() if v[2] == "COMIDA" and v[3] in ("TODO", franja)]
        n = 1 + poisson(2.2 if canal in ("MCDELIVERY", "PEDIDOSYA", "UBEREATS") else 0.8)
    elegidos = Counter(rng.choices(pool, weights=[PESO_PRODUCTO.get(p, 2) for p in pool], k=n))
    return [{"producto_id": p, "descripcion": MENU[p][0], "cantidad": q, "precio_cent": MENU[p][1],
             "tipo": "PRODUCTO", "recompensa_id": None} for p, q in elegidos.items()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. La hoja de respuestas: un ledger independiente
# MAGIC
# MAGIC Esta clase es una **segunda implementación** de las reglas, escrita en Python puro y **sin
# MAGIC nada en común** con el código de Gold (que irá en Spark). Si las dos llegan al mismo saldo
# MAGIC para los ~5,000 clientes, es una prueba fuerte de que Gold está bien.
# MAGIC
# MAGIC Supuestos que Gold debe respetar para coincidir:
# MAGIC
# MAGIC - Cada abono (acumulación, bienvenida, migración) es un **lote** con su fecha, y vence
# MAGIC   365 días después (R10). El vencimiento ocurre **al inicio del día**.
# MAGIC - Los canjes consumen **primero los lotes más viejos** (FIFO).
# MAGIC - Los puntos se acreditan en ≤24 h (R9), así que un canje solo puede usar lotes de días
# MAGIC   **anteriores**.
# MAGIC - El tope de 1,000 puntos/día (R8) aplica solo a la acumulación por compras, en orden
# MAGIC   cronológico y por día calendario de Guatemala (H5). Lo que excede el tope se pierde.

# COMMAND ----------

class LedgerEsperado:
    def __init__(self):
        self.lotes = []                     # [fecha, puntos_restantes], en orden cronológico
        self.total = Counter()
        self.acumulado_dia = Counter()
        self.canjeado_dia = Counter()

    def _vencer(self, dia):
        for lote in self.lotes:
            if lote[1] and lote[0] + timedelta(days=DIAS_VIGENCIA) <= dia:
                self.total["vencidos"] += lote[1]
                lote[1] = 0

    def disponible_para_canje(self, dia):
        self._vencer(dia)
        return sum(p for f, p in self.lotes if f < dia)

    def abonar(self, dia, puntos, tipo):
        self._vencer(dia)
        self.lotes.append([dia, puntos])
        self.total[tipo] += puntos

    def acumular(self, dia, puntos):
        otorgados = min(puntos, TOPE_DIARIO_ACUMULACION - self.acumulado_dia[dia])
        self.total["perdidos_por_tope"] += puntos - otorgados
        if otorgados > 0:
            self.acumulado_dia[dia] += otorgados
            self.abonar(dia, otorgados, "acumulados")

    def canjear(self, dia, puntos):
        self._vencer(dia)
        pendiente = puntos
        for lote in self.lotes:
            usar = min(lote[1], pendiente)
            lote[1] -= usar
            pendiente -= usar
        assert pendiente == 0, "el generador intentó canjear sin saldo"
        self.canjeado_dia[dia] += puntos
        self.total["canjeados"] += puntos

    def saldo(self, dia):
        self._vencer(dia)
        return sum(p for _, p in self.lotes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clientes (fuente C) y programa anterior (fuente F)
# MAGIC
# MAGIC Cada cliente tiene rasgos **ocultos** que definen su comportamiento: frecuencia de visita,
# MAGIC restaurante preferido, canales favoritos, qué tanto olvida mostrar el QR, qué tanto canjea y
# MAGIC si **abandona** el programa (*churn*). Estos rasgos no salen en los archivos, pero dejan
# MAGIC patrones en los datos que un modelo de ML podría aprender (Fase 4).
# MAGIC
# MAGIC Cuentas especiales:
# MAGIC - **E13 · 3 % de cuentas duplicadas:** misma persona (nombre y teléfono) con otro correo, que
# MAGIC   compra 1–3 veces para cobrar la bienvenida (H6).
# MAGIC - **E15 · 2 % de cuentas de Honduras:** la plataforma es compartida, pero no acumulan en GT (R20).
# MAGIC - **30 % venían de Puntos McDelivery** y traen saldo para migrar (R21).

# COMMAND ----------

codigos_usados, correos_usados, telefonos_usados = set(), set(), set()


def nuevo_correo(nombre, apellido):
    while True:
        base = f"{quitar_tildes(nombre).lower()}.{quitar_tildes(apellido).lower()}"
        correo = f"{base}{rng.randint(1, 9999)}@{rng.choice(DOMINIOS)}"
        if correo not in correos_usados:
            correos_usados.add(correo)
            return correo


def nuevo_telefono(prefijo="+502"):
    while True:
        tel = f"{prefijo}{rng.choice('345')}{rng.randint(0, 9_999_999):07d}"
        if tel not in telefonos_usados:
            telefonos_usados.add(tel)
            return tel


def fecha_registro(es_legado):
    if es_legado:        # los usuarios del programa anterior se registran en las primeras semanas
        dias = min(int(rng.expovariate(1 / 7)), 60)
    elif rng.random() < 0.35:   # campaña de lanzamiento
        dias = rng.randrange(30)
    else:
        dias = rng.randrange((FECHA_FIN - FECHA_INICIO).days - 21)
    dia = FECHA_INICIO + timedelta(days=dias)
    return datetime(dia.year, dia.month, dia.day, rng.randint(7, 22), rng.randrange(60), rng.randrange(60))


def rasgos_cliente():
    s = 0.9
    return {
        "visitas_semana": rng.lognormvariate(math.log(0.22) - s * s / 2, s),
        "restaurante": rng.choices(list(REST.values()), weights=[r["peso"] for r in REST.values()])[0],
        "pesos_canal": {c: p * rng.gammavariate(1.0, 1.0) for c, p in PESO_CANAL.items()},
        "p_desayuno": rng.betavariate(1.5, 6),
        "p_muestra_qr": rng.uniform(0.70, 0.98),
        "p_canje": rng.uniform(0.15, 0.80),
        "p_bienvenida": 0.6,
    }


clientes = []
for i in range(N_CLIENTES):
    es_legado = rng.random() < 0.30
    nombre, apellido = rng.choice(NOMBRES), rng.choice(APELLIDOS)
    registro = fecha_registro(es_legado)
    churn = None
    if rng.random() < 0.35:
        inicio_churn = (registro.date() - FECHA_INICIO).days + 14
        churn = FECHA_INICIO + timedelta(days=rng.randint(inicio_churn, (FECHA_FIN - FECHA_INICIO).days))
    clientes.append({
        "customer_id": f"C{i + 1:06d}", "nombre": nombre, "apellido": apellido,
        "correo": nuevo_correo(nombre, apellido),
        "telefono": nuevo_telefono() if rng.random() < 0.65 else None,
        "codigo": nuevo_codigo(codigos_usados), "registro": registro, "pais": "GT",
        "tipo": "NORMAL", "legado": rng.randint(20, 2_500) if es_legado else None,
        "churn": churn, **rasgos_cliente(),
    })

# E13: cuentas duplicadas de la misma persona
originales = [c for c in clientes if c["telefono"] and c["registro"].date() < FECHA_FIN - timedelta(days=90)]
for j, original in enumerate(rng.sample(originales, int(N_CLIENTES * 0.03))):
    registro = original["registro"] + timedelta(days=rng.randint(20, 200))
    if registro.date() > FECHA_FIN - timedelta(days=30):
        registro = datetime.combine(FECHA_FIN - timedelta(days=30), registro.time())
    clientes.append({
        **rasgos_cliente(), "customer_id": f"C{N_CLIENTES + j + 1:06d}",
        "nombre": original["nombre"], "apellido": original["apellido"],
        "correo": nuevo_correo(original["nombre"], original["apellido"]),
        "telefono": original["telefono"], "codigo": nuevo_codigo(codigos_usados),
        "registro": registro, "pais": "GT", "tipo": "DUPLICADO", "legado": None, "churn": None,
        "duplicado_de": original["customer_id"], "p_canje": 0.9, "p_bienvenida": 0.95,
        "p_muestra_qr": 0.99,
    })

# E15: cuentas de Honduras en la plataforma compartida
base_hn = len(clientes)
for k in range(int(N_CLIENTES * 0.02)):
    nombre, apellido = rng.choice(NOMBRES), rng.choice(APELLIDOS)
    clientes.append({
        **rasgos_cliente(), "customer_id": f"C{base_hn + k + 1:06d}", "nombre": nombre,
        "apellido": apellido, "correo": nuevo_correo(nombre, apellido),
        "telefono": nuevo_telefono("+504"), "codigo": nuevo_codigo(codigos_usados),
        "registro": fecha_registro(False), "pais": "HN", "tipo": "HONDURAS", "legado": None,
        "churn": None,
    })

print(f"[clientes] {len(clientes):,} cuentas "
      f"({Counter(c['tipo'] for c in clientes)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Simulación de visitas
# MAGIC
# MAGIC Cada cliente se simula **en orden cronológico**, desde que se registra hasta que abandona el
# MAGIC programa o termina el periodo. En cada visita:
# MAGIC
# MAGIC 1. Se elige restaurante, canal, hora y productos.
# MAGIC 2. Se decide si el cliente **se identifica** (muestra el QR en caja; en la app siempre se
# MAGIC    identifica). En canales excluidos a veces el agente de Call Center anota el código: esos
# MAGIC    tickets **traen código pero no deben acumular**.
# MAGIC 3. Si se identificó en un canal con canje y le alcanza el saldo, **puede canjear**.
# MAGIC 4. Se inyectan los errores que **cambian la verdad**: ticket anulado (E7/E9), código
# MAGIC    inexistente (E5), JSON sin un campo llave (E11).
# MAGIC 5. Se aplican las reglas al ledger esperado.

# COMMAND ----------

visitas = []
ledgers = {}
errores = defaultdict(list)       # E# -> ids afectados (hoja de respuestas)


def simular_cliente(c):
    ledger = LedgerEsperado()
    ledgers[c["customer_id"]] = ledger
    es_gt = c["pais"] == "GT"
    if es_gt and c["legado"] is not None and not c.get("legado_invalido"):
        ledger.abonar(max(FECHA_INICIO, c["registro"].date()), c["legado"] * FACTOR_MIGRACION, "migrados")

    # --- fechas de visita ---
    momentos = []
    if c["tipo"] == "DUPLICADO":
        for _ in range(rng.randint(1, 3)):
            momentos.append(c["registro"].date() + timedelta(days=rng.randint(0, 30)))
    elif c["tipo"] == "HONDURAS":
        for _ in range(rng.randint(0, 2)):
            momentos.append(c["registro"].date() + timedelta(days=rng.randint(0, 120)))
    else:
        fin = min(c["churn"] or FECHA_FIN, FECHA_FIN)
        t = 0.0
        while True:
            t += rng.expovariate(c["visitas_semana"] / 7)
            dia = c["registro"].date() + timedelta(days=int(t))
            if dia > fin:
                break
            momentos.append(dia)
    agenda = sorted(momento_aleatorio(d, c["p_desayuno"]) for d in momentos if d <= FECHA_FIN)
    agenda = [(dt, fr) for dt, fr in agenda if dt > c["registro"]]

    bienvenida_usada, ya_compro = False, False
    for dt, franja in agenda:
        dia = dt.date()
        restaurante = restaurante_para(dia, c["restaurante"])
        canal = canal_para(restaurante, c["pesos_canal"], solo_app=c["tipo"] == "HONDURAS")
        origen = "APP" if canal in CANALES_APP else "POS"
        lineas = armar_lineas(canal, franja)
        if origen == "POS" and canal not in CANALES_EXCLUIDOS and rng.random() < 0.05:
            lineas.append({"producto_id": DONACION[0], "descripcion": DONACION[1], "cantidad": 1,
                           "precio_cent": rng.choice([100, 200, 300, 500]), "tipo": "DONACION",
                           "recompensa_id": None})
        productos_cent = sum(l["precio_cent"] * l["cantidad"] for l in lineas if l["tipo"] == "PRODUCTO")

        if origen == "APP":
            identificado = True
        elif canal in CANALES_EXCLUIDOS:
            identificado = rng.random() < 0.35
        else:
            identificado = rng.random() < c["p_muestra_qr"]

        # --- canjes (solo GT, identificado y en canal que permite canje) ---
        ofertas = []
        if es_gt and identificado and canal in CANALES_CON_CANJE:
            if not bienvenida_usada and rng.random() < c["p_bienvenida"]:
                ofertas.append(("RW-BIENVENIDA", "P-QUESOB", 0))
            saldo = ledger.disponible_para_canje(dia)
            margen_dia = TOPE_DIARIO_CANJE - ledger.canjeado_dia[dia]
            if rng.random() < c["p_canje"]:
                cuantos = rng.choices([1, 2, 3], weights=[80, 15, 5])[0]
                for _ in range(cuantos):
                    if len(ofertas) >= MAX_OFERTAS_POR_PEDIDO:
                        break
                    usados = sum(o[2] for o in ofertas)
                    alcanzan = [r for r in recompensas_vigentes(dia, franja)
                                if r[2] <= min(saldo, margen_dia) - usados]
                    if not alcanzan:
                        break
                    ofertas.append(rng.choices(alcanzan, weights=[r[2] for r in alcanzan])[0])
            e10 = False
            if ofertas and canal == "MCDELIVERY" and productos_cent < MINIMO_CANJE_DELIVERY_CENT[franja]:
                if rng.random() < 0.2:
                    e10 = True           # la app dejó pasar un canje bajo el mínimo (R15)
                else:
                    ofertas = []
        else:
            e10 = False

        for rid, pid, puntos in ofertas:
            lineas.append({"producto_id": pid, "descripcion": f"CANJE {MENU[pid][0]}", "cantidad": 1,
                           "precio_cent": 0, "tipo": "CANJE", "recompensa_id": rid})

        # --- errores que cambian la verdad ---
        anulada = rng.random() < 0.02                                        # E7 / E9
        codigo_invalido = (origen == "POS" and identificado and not ofertas
                           and canal in CANALES_POS_ACUMULAN and rng.random() < 0.01)   # E5
        campo_faltante = None
        if origen == "APP" and not ofertas and rng.random() < 0.01:          # E11
            campo_faltante = rng.choice(["customer_id", "store_id", "created_at"])

        visita = {"cliente": c["customer_id"], "codigo": c["codigo"] if identificado else None,
                  "restaurante": restaurante["id"], "dt": dt, "canal": canal, "origen": origen,
                  "franja": franja, "lineas": lineas, "anulada": anulada,
                  "codigo_invalido": codigo_invalido, "campo_faltante": campo_faltante, "e10": e10}
        visitas.append(visita)

        # --- reglas sobre el ledger esperado ---
        if ofertas:
            if any(o[0] == "RW-BIENVENIDA" for o in ofertas):
                bienvenida_usada = True
            puntos_canje = sum(o[2] for o in ofertas)
            if puntos_canje:
                ledger.canjear(dia, puntos_canje)    # R14: se descuentan aunque la orden se anule
        acredita = (es_gt and identificado and canal not in CANALES_EXCLUIDOS and not anulada
                    and not codigo_invalido and campo_faltante is None and productos_cent > 0)
        if acredita:
            ledger.acumular(dia, (productos_cent * PUNTOS_POR_QUETZAL + 99) // 100)   # R1 + R2
            if not ya_compro:
                ledger.abonar(dia, PUNTOS_BIENVENIDA, "bienvenida")                  # R17
                ya_compro = True


# E18: saldos negativos en el programa anterior (se marcan antes de simular: no migran)
con_legado = [c for c in clientes if c["legado"] is not None]
for c in rng.sample(con_legado, max(1, int(len(con_legado) * 0.005))):
    c["legado"] = -rng.randint(10, 500)
    c["legado_invalido"] = True

for c in clientes:
    simular_cliente(c)

# Tickets anónimos: gente que compra sin cuenta en el programa
dia = FECHA_INICIO
while dia <= FECHA_FIN:
    for r in REST.values():
        if r["apertura"] > dia:
            continue
        for _ in range(poisson(1.5)):
            dt, franja = momento_aleatorio(dia, 0.2)
            canal = canal_para(r, {c: p for c, p in PESO_CANAL.items() if c not in CANALES_APP})
            visitas.append({"cliente": None, "codigo": None, "restaurante": r["id"], "dt": dt,
                            "canal": canal, "origen": "POS", "franja": franja,
                            "lineas": armar_lineas(canal, franja), "anulada": rng.random() < 0.02,
                            "codigo_invalido": False, "campo_faltante": None, "e10": False})
    dia += timedelta(days=1)

visitas.sort(key=lambda v: (v["dt"], v["restaurante"]))
print(f"[visitas] {len(visitas):,} tickets "
      f"({Counter(v['origen'] for v in visitas)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Identificadores y errores "cosméticos"
# MAGIC
# MAGIC Estos errores **no cambian la verdad**: el dato correcto sigue ahí, pero mal escrito. Silver
# MAGIC debe corregirlos sin perder ningún ticket.

# COMMAND ----------

VARIANTES_CANAL = {"AUTOMAC": ["Automac", "AUTO MAC", "drive"], "MOSTRADOR": ["mostrador", "Mostrador "],
                   "KIOSCO": ["kiosko", "Kiosco"], "MCCAFE": ["McCafe", "MCCAFÉ"],
                   "POSTRES": ["postres", "Centro de Postres"], "PEDIDOSYA": ["Pedidos Ya"],
                   "UBEREATS": ["Uber Eats"], "CALLCENTER": ["Call Center"], "WHATSAPP": ["Whatsapp"]}
codigos_existentes = {c["codigo"] for c in clientes}
secuencia = Counter()

for v in visitas:
    if v["origen"] == "POS":
        clave = (v["restaurante"], v["dt"].date())
        secuencia[clave] += 1
        v["id"] = f"{v['restaurante']}-{v['dt']:%Y%m%d}-{secuencia[clave]:06d}"
        v["canal_texto"] = v["canal"]
        if rng.random() < 0.05:                                               # E2
            v["canal_texto"] = rng.choice(VARIANTES_CANAL[v["canal"]])
            errores["E2_canal_mal_escrito"].append(v["id"])
        if v["codigo_invalido"]:                                              # E5
            v["codigo"] = nuevo_codigo(codigos_existentes)
            errores["E5_codigo_inexistente"].append(v["id"])
        elif v["codigo"] and rng.random() < 0.04:                             # E4
            v["codigo"] = rng.choice([v["codigo"].lower(), f" {v['codigo']}", f"{v['codigo']} ",
                                      f" {v['codigo'].lower()}"])
            errores["E4_codigo_formato"].append(v["id"])
        v["linea_coma"] = rng.randrange(len(v["lineas"])) if rng.random() < 0.03 else None   # E3
        if v["linea_coma"] is not None:
            errores["E3_coma_decimal"].append(v["id"])
        v["total_cent"] = sum(l["precio_cent"] * l["cantidad"] for l in v["lineas"])
        v["total_reportado"] = v["total_cent"]
        if rng.random() < 0.01:                                               # E6
            v["total_reportado"] += rng.choice([-1, 1]) * rng.randint(500, 2000)
            errores["E6_total_no_cuadra"].append(v["id"])
        if v["anulada"]:
            errores["E7_ticket_anulado"].append(v["id"])
    else:
        dt_utc = v["dt"] + timedelta(hours=DESFASE_UTC_HORAS)
        clave = ("APP", dt_utc.date())
        secuencia[clave] += 1
        v["id"] = f"APP-{dt_utc:%Y%m%d}{secuencia[clave]:05d}"
        v["dt_utc"] = dt_utc
        if v["anulada"]:
            errores["E9_pedido_cancelado"].append(v["id"])
        if v["e10"]:
            errores["E10_canje_bajo_minimo"].append(v["id"])
        if v["campo_faltante"]:
            errores["E11_campo_faltante"].append(f"{v['id']}:{v['campo_faltante']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Escritura de las fuentes A y B: POS (CSV) y app (JSON Lines)
# MAGIC
# MAGIC - **POS:** un CSV por día con los tickets de todos los restaurantes, en
# MAGIC   `pos/fecha=AAAA-MM-DD/`. En la realidad serían ~126 archivos diarios (uno por restaurante);
# MAGIC   aquí se consolidan para no crear 11,000 archivos diminutos: el *small files problem*
# MAGIC   hace lentísimo a Spark.
# MAGIC - **E1:** en el 2 % de los días, un restaurante **reenvía** su parte en un segundo archivo.
# MAGIC - **App:** un `.jsonl` por día **UTC** (no hora de Guatemala). Un pedido cancelado genera un
# MAGIC   **segundo evento** con el mismo `order_id` (E9), y el 3 % de los eventos llega duplicado (E8).

# COMMAND ----------

COLUMNAS_POS = ["ticket_id", "restaurante_id", "fecha_hora_local", "canal", "codigo_lealtad",
                "linea_num", "producto_id", "descripcion", "cantidad", "precio_unitario_q",
                "tipo_linea", "recompensa_id", "total_ticket_q", "estado"]


def filas_pos(v, estado):
    filas = []
    for n, l in enumerate(v["lineas"]):
        coma = v["linea_coma"] == n
        filas.append([v["id"], v["restaurante"], f"{v['dt']:%Y-%m-%d %H:%M:%S}", v["canal_texto"],
                      v["codigo"] or "", n + 1, l["producto_id"], l["descripcion"], l["cantidad"],
                      quetzales(l["precio_cent"], coma), l["tipo"], l["recompensa_id"] or "",
                      quetzales(v["total_reportado"], coma and rng.random() < 0.5), estado])
    return filas


def escribir_csv(ruta, columnas, filas):
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(columnas)
        escritor.writerows(filas)


pos_por_dia, app_por_dia = defaultdict(list), defaultdict(list)
for v in visitas:
    if v["origen"] == "POS":
        pos_por_dia[v["dt"].date()].append(v)
    else:
        app_por_dia[v["dt_utc"].date()].append(v)

archivos = Counter()
for dia, tickets in sorted(pos_por_dia.items()):
    carpeta = os.path.join(RUTA_LANDING, "pos", f"fecha={dia}")
    filas = [f for v in tickets for f in filas_pos(v, "COMPLETADA")]
    filas += [f for v in tickets if v["anulada"] for f in filas_pos(v, "ANULADA")]
    escribir_csv(os.path.join(carpeta, f"pos_{dia}.csv"), COLUMNAS_POS, filas)
    archivos["pos"] += 1
    if rng.random() < 0.02:                                                   # E1
        rest = rng.choice(sorted({v["restaurante"] for v in tickets}))
        reenvio = [f for f in filas if f[1] == rest]
        escribir_csv(os.path.join(carpeta, f"pos_{dia}_reenvio_{rest}.csv"), COLUMNAS_POS, reenvio)
        errores["E1_archivo_reenviado"].append(f"pos_{dia}_reenvio_{rest}.csv")
        archivos["pos_reenvio"] += 1


def evento_app(v, status, actualizado):
    evento = {
        "order_id": v["id"], "customer_id": v["cliente"], "store_id": int(v["restaurante"][1:]),
        "channel": v["canal"], "created_at": f"{v['dt_utc']:%Y-%m-%dT%H:%M:%SZ}",
        "updated_at": f"{actualizado:%Y-%m-%dT%H:%M:%SZ}", "status": status,
        "items": [{"sku": l["producto_id"], "name": l["descripcion"], "qty": l["cantidad"],
                   "unit_price": l["precio_cent"] / 100,
                   "type": "REWARD" if l["tipo"] == "CANJE" else "PRODUCT",
                   **({"reward_id": l["recompensa_id"]} if l["recompensa_id"] else {})}
                  for l in v["lineas"]],
        "total": sum(l["precio_cent"] * l["cantidad"] for l in v["lineas"]) / 100,
    }
    if v["campo_faltante"]:
        evento.pop(v["campo_faltante"])
    return evento


for dia, pedidos in sorted(app_por_dia.items()):
    lineas_json = []
    for v in pedidos:
        entregado = v["dt_utc"] + timedelta(minutes=rng.randint(20, 50))
        eventos = [evento_app(v, "DELIVERED" if v["canal"] == "MCDELIVERY" else "PICKED_UP", entregado)]
        if v["anulada"]:
            eventos.append(evento_app(v, "CANCELLED", entregado + timedelta(minutes=rng.randint(30, 120))))
        for e in eventos:
            lineas_json.append(json.dumps(e, ensure_ascii=False))
            if rng.random() < 0.03:                                           # E8
                lineas_json.append(json.dumps(e, ensure_ascii=False))
                errores["E8_evento_duplicado"].append(v["id"])
    ruta = os.path.join(RUTA_LANDING, "app", f"fecha={dia}", "pedidos.jsonl")
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas_json) + "\n")
    archivos["app"] += 1

print(f"[landing] archivos escritos: {dict(archivos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Escritura de las fuentes C, D, E y F
# MAGIC
# MAGIC - **CRM (C):** un **arreglo JSON** (no JSON Lines), como lo devolvería una API. Bronze tendrá
# MAGIC   que leerlo con `multiLine`. Trae E12 (correos mal escritos) y E14 (teléfonos en 4 formatos).
# MAGIC - **Restaurantes (D):** banderas `SI`/`NO` y E16 (departamentos sin tilde).
# MAGIC - **Catálogo (E):** versionado con `vigente_desde` / `vigente_hasta`.
# MAGIC - **Legado (F):** fechas en formato `dd/mm/aaaa` y correos en minúscula. Trae E17 (correos que
# MAGIC   nunca se registraron en el programa nuevo) y E18 (saldos negativos).

# COMMAND ----------

def formato_telefono(c):
    tel = c["telefono"]
    if tel is None or rng.random() >= 0.30:
        return tel
    errores["E14_telefono_formato"].append(c["customer_id"])
    pais, numero = tel[1:4], tel[4:]
    return rng.choice([f"{numero[:4]}-{numero[4:]}", numero, f"({pais}) {numero[:4]}-{numero[4:]}"])


def formato_correo(c):
    correo = c["correo"]
    if rng.random() < 0.05:
        errores["E12_correo_formato"].append(c["customer_id"])
        return rng.choice([correo.upper(), correo.capitalize(), f" {correo}", f"{correo} "])
    return correo


crm = []
for c in clientes:
    registro_utc = c["registro"] + timedelta(hours=DESFASE_UTC_HORAS)
    crm.append({"customer_id": c["customer_id"], "email": formato_correo(c),
                "first_name": c["nombre"], "last_name": c["apellido"],
                "phone": formato_telefono(c), "loyalty_code": c["codigo"],
                "registered_at": f"{registro_utc:%Y-%m-%dT%H:%M:%SZ}", "country": c["pais"]})
    if c["tipo"] == "DUPLICADO":
        errores["E13_cuenta_duplicada"].append(f"{c['customer_id']}~{c['duplicado_de']}")
    if c["tipo"] == "HONDURAS":
        errores["E15_cliente_honduras"].append(c["customer_id"])
os.makedirs(os.path.join(RUTA_LANDING, "crm"), exist_ok=True)
with open(os.path.join(RUTA_LANDING, "crm", "clientes.json"), "w", encoding="utf-8") as f:
    json.dump(crm, f, ensure_ascii=False, indent=1)

sin_tilde = set(rng.sample([r[0] for r in RESTAURANTES if quitar_tildes(r[2]) != r[2]], 3))   # E16
errores["E16_departamento_sin_tilde"] = sorted(sin_tilde)
escribir_csv(os.path.join(RUTA_LANDING, "maestros", "restaurantes.csv"),
             ["restaurante_id", "nombre", "departamento", "municipio", "tiene_automac",
              "tiene_mcdelivery", "fecha_apertura"],
             [[r[0], r[1], quitar_tildes(r[2]) if r[0] in sin_tilde else r[2], r[3],
               "SI" if r[4] else "NO", "SI" if r[5] else "NO", r[6]] for r in RESTAURANTES])

escribir_csv(os.path.join(RUTA_LANDING, "maestros", "catalogo_recompensas.csv"),
             ["recompensa_id", "producto_id", "producto", "puntos", "precio_referencia_q",
              "vigente_desde", "vigente_hasta"],
             [[rid, pid, MENU[pid][0], pts, quetzales(MENU[pid][1]), desde, hasta or ""]
              for rid, pid, pts, desde, hasta in CATALOGO_RECOMPENSAS])

legado = []
for c in con_legado:
    ultima = FECHA_INICIO - timedelta(days=rng.randint(1, 400))
    legado.append([c["correo"], c["legado"], f"{ultima:%d/%m/%Y}"])
    if c.get("legado_invalido"):
        errores["E18_saldo_legado_negativo"].append(c["correo"])
for _ in range(int(len(con_legado) * 0.15 / 0.85)):                          # E17
    nombre, apellido = rng.choice(NOMBRES), rng.choice(APELLIDOS)
    correo = nuevo_correo(nombre, apellido)
    ultima = FECHA_INICIO - timedelta(days=rng.randint(1, 700))
    legado.append([correo, rng.randint(20, 2_500), f"{ultima:%d/%m/%Y}"])
    errores["E17_legado_no_registrado"].append(correo)
rng.shuffle(legado)
escribir_csv(os.path.join(RUTA_LANDING, "legado", "puntos_mcdelivery.csv"),
             ["email", "saldo_puntos", "ultima_compra"], legado)

print(f"[landing] crm: {len(crm):,} cuentas · restaurantes: {len(RESTAURANTES)} · "
      f"catálogo: {len(CATALOGO_RECOMPENSAS)} versiones · legado: {len(legado):,} saldos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Hoja de respuestas (Volume `control`)
# MAGIC
# MAGIC - `saldos_esperados.csv`: por cliente GT, cuánto acumuló, canjeó, venció, perdió por tope, y
# MAGIC   su **saldo al cierre** (20-sep-2026).
# MAGIC - `errores_inyectados.json`: qué registros traen cada error, para verificar que Silver los
# MAGIC   atrapa todos.
# MAGIC - `resumen_esperado.json`: totales del programa.

# COMMAND ----------

filas_saldos, totales = [], Counter()
for c in clientes:
    if c["pais"] != "GT":
        continue
    ledger = ledgers[c["customer_id"]]
    saldo = ledger.saldo(FECHA_FIN)
    t = ledger.total
    filas_saldos.append([c["customer_id"], t["acumulados"], t["bienvenida"], t["migrados"],
                         t["canjeados"], t["vencidos"], t["perdidos_por_tope"], saldo])
    totales.update(t)
    totales["saldo_final"] += saldo
    # cuadre contable: todo lo que entró = lo que salió + lo que queda
    assert t["acumulados"] + t["bienvenida"] + t["migrados"] == t["canjeados"] + t["vencidos"] + saldo

escribir_csv(os.path.join(RUTA_CONTROL, "saldos_esperados.csv"),
             ["customer_id", "puntos_acumulados", "puntos_bienvenida", "puntos_migrados",
              "puntos_canjeados", "puntos_vencidos", "puntos_perdidos_por_tope", "saldo_final"],
             filas_saldos)

with open(os.path.join(RUTA_CONTROL, "errores_inyectados.json"), "w", encoding="utf-8") as f:
    json.dump({k: sorted(set(v)) for k, v in sorted(errores.items())}, f, ensure_ascii=False, indent=1)

resumen = {
    "semilla": SEMILLA, "fecha_inicio": str(FECHA_INICIO), "fecha_fin": str(FECHA_FIN),
    "cuentas_crm": len(crm), "clientes_gt": len(filas_saldos),
    "tickets_pos": sum(v["origen"] == "POS" for v in visitas),
    "pedidos_app": sum(v["origen"] == "APP" for v in visitas),
    "tickets_anonimos": sum(v["cliente"] is None for v in visitas),
    "errores": {k: len(set(v)) for k, v in sorted(errores.items())},
    "puntos": dict(totales),
}
with open(os.path.join(RUTA_CONTROL, "resumen_esperado.json"), "w", encoding="utf-8") as f:
    json.dump(resumen, f, ensure_ascii=False, indent=1)

print(json.dumps(resumen, ensure_ascii=False, indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Vistazo rápido a Landing
# MAGIC
# MAGIC Solo en Databricks: se listan las carpetas del Volume para comprobar que todo llegó.

# COMMAND ----------

if EN_DATABRICKS:
    for sub in ["pos", "app", "crm", "maestros", "legado"]:
        contenido = dbutils.fs.ls(f"{RUTA_LANDING}/{sub}")  # noqa: F821
        print(f"{sub:<9} {len(contenido):>4} elementos · ej. {contenido[0].name}")
