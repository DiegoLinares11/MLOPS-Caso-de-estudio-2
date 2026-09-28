# Fase 5 — Evaluación (CRISP-DM) y prueba funcional (POC)

**Pregunta de la fase:** ¿lo que construimos cumple lo que el negocio necesita?

El notebook [05_evaluacion.py](../notebooks/05_evaluacion.py) es la **prueba funcional (POC)**
del sistema de puntos que pide el reporte. Es el único notebook que lee la hoja de respuestas
del generador (Volume `control`): el pipeline (01–04) nunca la lee, porque en producción no
existe. Aquí se usa para evaluar el pipeline **desde afuera**.

**Resultado: 39 de 39 pruebas OK** (tabla `eval_resultados_poc` en Unity Catalog).

---

## Parte A · Pruebas funcionales por regla (12/12)

Cada prueba toma **todos** los casos de los datos donde aplica la regla, calcula lo esperado de
forma independiente y cuenta las fallas.

| Regla | Prueba | Casos | Fallas | Ejemplo |
|---|---|---|---|---|
| R1 + R2 | Puntos = `ceil(monto de productos × 10)` | Todas las acumulaciones | 0 | Q12.75 × 10 = 127.5 → **128** puntos |
| R4 | Las donaciones no suman puntos | 1,120 tickets con donación | 0 | |
| R7 | Canales excluidos no acumulan, **aunque** el agente anotara el código | 1,378 tickets | 0 | |
| R20 | Las cuentas de Honduras no acumulan, **aunque** compraron en Guatemala | 92 compras | 0 | |
| R8 | Otorgado por día = `min(1000, calculado)` | 28,883 cliente-días | 0 | C002481 calculó 5,430 el 8-may-2026 y recibió **1,000** |
| R10 | Cada lote vence a los 365 días | 1,399 lotes vencidos | 0 | |
| R10 | Lo vencido nunca supera lo abonado | 1,399 lotes | 0 | |
| R14 | Un canje se descuenta aunque la orden se anule | 104 canjes | 0 | |
| R16 | Costo según el catálogo **vigente el día del canje** | 245 canjes de Big Tasty | 0 | 158 a 7,000 y 87 a 7,500 |
| R17 | Una bienvenida de 1,000 en la primera compra | 4,336 clientes | 0 | |
| R17 | Bienvenidas = clientes que acumularon alguna vez | 4,336 | 0 | |
| R21 | Migración = saldo anterior × 10 | 1,423 cuentas | 0 | 252 saldos siguen sin reclamar |

## Parte B · Calidad de datos (18/18)

No basta con que los **conteos** coincidan: dos errores podrían compensarse. Se comparan los
**registros**, igual que se evalúa un clasificador con *precision* y *recall*:

| Error | Inyectados | Detectados | Aciertos | Faltantes | Falsas alarmas |
|---|---|---|---|---|---|
| E2 canal mal escrito | 2,454 | 2,454 | 2,454 | 0 | 0 |
| E3 coma decimal | 1,494 | 1,494 | 1,494 | 0 | 0 |
| E4 código mal escrito | 998 | 998 | 998 | 0 | 0 |
| E5 código inexistente | 167 | 167 | 167 | 0 | 0 |
| E6 total que no cuadra | 445 | 445 | 445 | 0 | 0 |
| E7 ticket anulado | 946 | 946 | 946 | 0 | 0 |
| E8 evento duplicado (sin los de E11) | 237 | 237 | 237 | 0 | 0 |
| E9 pedido cancelado (sin los de E11) | 151 | 151 | 151 | 0 | 0 |
| E10 canje bajo el mínimo (lo detecta Gold) | 33 | 33 | 33 | 0 | 0 |
| E11 campo faltante (se compara pedido **y** campo) | 55 | 55 | 55 | 0 | 0 |
| E12 correo mal escrito | 272 | 272 | 272 | 0 | 0 |
| E13 cuenta duplicada (se compara el **par** duplicada–principal) | 150 | 150 | 150 | 0 | 0 |
| E14 teléfono en otro formato | 990 | 990 | 990 | 0 | 0 |
| E15 cuenta de Honduras | 100 | 100 | 100 | 0 | 0 |
| E16 departamento sin tilde | 3 | 3 | 3 | 0 | 0 |
| E17 saldo anterior sin registrar | 252 | 252 | 252 | 0 | 0 |
| E18 saldo anterior negativo | 7 | 7 | 7 | 0 | 0 |
| E1 archivo reenviado (filas eliminadas) | 70 | 70 | — | — | — |

## Parte C · Saldos (8/8)

Gold (Spark) contra la contabilidad independiente del generador (Python puro), **cliente por
cliente**:

| Métrica | Total esperado | Total Gold | Clientes con diferencia |
|---|---|---|---|
| Acumulados | 18,581,394 | 18,581,394 | 0 |
| Bienvenida | 4,336,000 | 4,336,000 | 0 |
| Migrados | 17,835,600 | 17,835,600 | 0 |
| Canjeados | 20,729,000 | 20,729,000 | 0 |
| Vencidos | 8,097,012 | 8,097,012 | 0 |
| Perdidos por tope | 5,277,554 | 5,277,554 | 0 |
| **Saldo final** | **11,926,982** | **11,926,982** | **0** |

Dos implementaciones escritas por separado, en lenguajes y motores distintos, coinciden en
**5,150 clientes × 7 métricas**.

## Parte D · ¿El recomendador aprendió los gustos reales? (1/1)

El modelo nunca vio el segmento de gustos de los clientes. Se mide si entre sus 3
recomendaciones hay alguna recompensa de su gusto, contra recomendar a todos las 3 más canjeadas
(papas medianas, McNuggets 6 y McFlurry).

| Segmento | Clientes | Modelo | Baseline popular | Top-1 del modelo es de su gusto |
|---|---|---|---|---|
| Res | 1,102 | **0.952** | 0.000 | 0.149 |
| Pollo | 777 | 1.000 | 1.000 | 0.892 |
| Café y postres | 539 | 0.523 | **1.000** | 0.091 |
| Familia | 607 | 0.428 | **1.000** | 0.099 |
| Desayuno | 674 | 0.123 | 0.000 | 0.039 |
| **Total** | **3,699** | **0.663** | **0.520** | |

### Interpretación

- **El modelo gana en total**, sobre todo en el segmento más grande (res): le ofrece la
  quesoburguesa, que el baseline nunca muestra.
- **Pierde en café/postres y familia**, porque la lista popular ya incluye el McFlurry. El modelo
  prioriza lo barato (ver Fase 4) y a veces cambia el McFlurry por papas.
- **El segmento desayuno queda desatendido por los dos.** No es un problema del algoritmo, **es del
  catálogo**: las recompensas de desayuno cuestan 5,000 y 7,500 puntos, y la única barata (el café,
  3,000) salió del catálogo en junio. **El 18 % de los clientes no tiene una recompensa accesible
  que le guste.** Lo mismo le pasa a las familias: no hay recompensa de Cajita Feliz.

### Mejoras propuestas al recomendador

1. **Reservar un espacio para diversidad:** posiciones 1 y 2 del modelo y la 3 para la recompensa
   popular que el modelo no incluyó. Recupera café/postres y familia sin perder res.
2. **Cambiar el objetivo** si el negocio lo decide: ponderar el canje por el valor de la recompensa
   o por la probabilidad de una visita adicional (Fase 4).
3. **Revisar el catálogo con el cliente:** una recompensa de desayuno de 3,000 puntos y una de
   Cajita Feliz atenderían a un tercio de los clientes.

---

## Conclusión de la Fase 5

| Objetivo del negocio | ¿Se cumple? | Evidencia |
|---|---|---|
| Documentar el sistema que nadie documentó | Sí | 22 reglas (Fase 1), contratos de datos y arquitectura (Fase 2) |
| Replicar el sistema de puntos con datos confiables | Sí | 12 pruebas funcionales por regla, 0 fallas |
| Detectar los problemas de calidad de datos | Sí | 17 tipos de error, 0 faltantes y 0 falsas alarmas |
| Calcular los saldos correctamente | Sí | 5,150 clientes × 7 métricas, 0 diferencias |
| Proponer ML en el pipeline | Sí, con matices | El recomendador supera al baseline en total (+39 % en hit@1 sobre canjes reales y +0.14 en afinidad con el gusto), pero deja sin atender al segmento desayuno por un problema de catálogo |

**Límite de esta evaluación:** los datos son sintéticos. La POC demuestra que el **pipeline**
aplica las reglas correctamente; no demuestra que las reglas sean las que McDonald's usa, porque
esas se reconstruyeron desde fuentes públicas con supuestos (H1–H8). Validarlas con el cliente es
el primer paso de la Fase 6.
