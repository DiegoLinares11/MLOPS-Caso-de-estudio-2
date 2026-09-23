# Fase 2 — Entendimiento de los datos (CRISP-DM)

**Objetivo de la fase:** identificar **qué sistemas generan datos** en el programa MiMcDonald's,
qué columnas produce cada uno (su *contrato de datos*) y qué problemas de calidad traen.

Como el cliente no nos entregó datos, cada fuente se replica con **datos sintéticos** que:

1. **respetan las reglas** documentadas en la Fase 1 (R1–R22), y
2. **traen errores inyectados a propósito**, iguales a los que tendría un sistema real.

Así sabemos de antemano cuál es la "verdad" y podemos comprobar que el pipeline la recupera.

---

## 1. ¿Qué está documentado y qué es inferido?

Un consultor tiene que separar lo que **sabe** de lo que **supone**. En el diagrama los nodos
inferidos van con borde punteado.

| Elemento | Evidencia | Tipo |
|---|---|---|
| Canales: Mostrador, AutoMac, Pantallas digitales, McCafé, Centros de Postre, McDelivery | Términos y Condiciones (T&C) | Documentado |
| Identificación del cliente en caja por **código QR o alfanumérico** | Nota de lanzamiento [3] | Documentado |
| Canales excluidos: terceros, Call Center, WhatsApp | T&C y [3] | Documentado |
| App McDonald's GT como punto de registro, saldo y canje | T&C, sitio MiMcDonald's | Documentado |
| Migración desde Puntos McDelivery | T&C | Documentado |
| Plataforma digital compartida con Honduras | Mismo dominio de T&C para GT y HN | Inferido (fuerte) |
| El POS envía las transacciones por lotes (batch) | Acreditación en "máximo 24 horas" (R9) | Inferido |
| Existe un motor central de reglas con ledger | Vencimiento por lote (R10) y aviso a 30 días (R11) | Inferido |
| Nube del franquiciado | McDonald's corporativo usa Google Cloud, pero **no hay información pública de McDonald's Mesoamérica** | Desconocido → hallazgo H8 |

> **H8 (nuevo hallazgo):** la arquitectura del franquiciado no es pública. Lo que se sabe de
> Google Cloud aplica a McDonald's corporativo y no necesariamente a McDonald's Mesoamérica.
> Esto se reporta como supuesto y no como hecho.

## 2. Fuentes de datos y contratos

Hay 6 fuentes. Los formatos se eligieron **distintos a propósito**: en un sistema real cada
proveedor entrega sus datos a su manera, y el trabajo de Silver es unificarlos.

### A. POS del restaurante — `landing/pos/fecha=AAAA-MM-DD/<restaurante>.csv`

Cada restaurante sube un archivo por día (batch, H4). **Una fila por línea del ticket**, que
es el formato típico de exportación de un POS.

| Columna | Tipo | Ejemplo | Nota |
|---|---|---|---|
| `ticket_id` | texto | `R012-20251003-000457` | |
| `restaurante_id` | texto | `R012` | |
| `fecha_hora_local` | texto | `2025-10-03 13:45:10` | **Hora local sin zona horaria** |
| `canal` | texto | `AUTOMAC` | Incluye canales excluidos (terceros, Call Center, WhatsApp) |
| `codigo_lealtad` | texto, nulo | `K7Q2M9XA` | El POS solo conoce el código QR, **no el correo del cliente** |
| `linea_num` | entero | `1` | |
| `producto_id` | texto | `P-BIGMAC` | |
| `descripcion` | texto | `Big Mac` | |
| `cantidad` | entero | `2` | |
| `precio_unitario_q` | texto | `45.50` | Precio con IVA incluido (R3) |
| `tipo_linea` | texto | `PRODUCTO` / `DONACION` / `CANJE` | R4, R5 |
| `recompensa_id` | texto, nulo | `RW-PAPAS-M` | Solo si `tipo_linea = CANJE` |
| `total_ticket_q` | texto | `91.00` | |
| `estado` | texto | `COMPLETADA` / `ANULADA` | Una anulación llega como otra fila del mismo ticket |

### B. App McDonald's GT — `landing/app/fecha=AAAA-MM-DD/pedidos.jsonl`

Pedidos de McDelivery y "pide y recoge" en la app. Es **JSON anidado**: un pedido trae dentro
sus líneas. Los nombres de campo están **en inglés**, como suele entregarlos el proveedor de la
app.

```json
{
  "order_id": "APP-2025100300123",
  "customer_id": "C004821",
  "store_id": 12,
  "channel": "MCDELIVERY",
  "created_at": "2025-10-03T19:45:10Z",
  "status": "DELIVERED",
  "items": [
    {"sku": "P-BIGMAC", "name": "Big Mac", "qty": 1, "unit_price": 45.5, "type": "PRODUCT"},
    {"sku": "P-PAPAS-M", "name": "Papas medianas", "qty": 1, "unit_price": 0, "type": "REWARD", "reward_id": "RW-PAPAS-M"}
  ],
  "total": 45.5
}
```

Diferencias con el POS que Silver debe resolver:

- `created_at` viene en **UTC**, mientras que el POS usa hora local (Guatemala es UTC−6). Esto importa para el tope diario (H5).
- `store_id` es un entero (`12`); en el POS es texto (`R012`).
- La app sí conoce el `customer_id`; el POS solo conoce el `codigo_lealtad`.

### C. CRM de clientes — `landing/crm/clientes.json`

Exportación de las cuentas registradas en la app.

| Campo | Tipo | Nota |
|---|---|---|
| `customer_id` | texto | Llave técnica |
| `email` | texto | Llave de negocio (R18) |
| `first_name`, `last_name` | texto | Obligatorios |
| `phone` | texto, nulo | Opcional (R18) |
| `loyalty_code` | texto | Código QR / alfanumérico que se muestra en caja |
| `registered_at` | timestamp UTC | Fecha de registro en la app |
| `country` | texto | `GT` / `HN` (plataforma compartida) |

### D. Maestro de restaurantes — `landing/maestros/restaurantes.csv`

`restaurante_id`, `nombre`, `departamento`, `municipio`, `tiene_automac`, `tiene_mcdelivery`, `fecha_apertura`

### E. Catálogo de recompensas — `landing/maestros/catalogo_recompensas.csv`

El catálogo cambia sin previo aviso (R16), así que viene **versionado**: cada fila es una
versión de la recompensa, con su rango de vigencia.

`recompensa_id`, `producto`, `puntos`, `precio_referencia_q`, `vigente_desde`, `vigente_hasta` (nulo = vigente)

### F. Programa anterior — `landing/legado/puntos_mcdelivery.csv`

Foto de los saldos al **27 de agosto de 2025**, día del lanzamiento: `email`, `saldo_puntos`
(a 1 pt por Q1), `ultima_compra`.

## 3. Errores inyectados y dónde se corrigen

Cada error tiene una **regla que lo detecta**. Los registros que no se pueden corregir **no se
borran**: van a una tabla de **cuarentena** con el motivo, para que se puedan auditar.

| # | Error inyectado | Fuente | Frecuencia aprox. | Cómo lo resuelve Silver |
|---|---|---|---|---|
| E1 | Archivo del POS **reenviado** (tickets duplicados) | A | 2 % de archivos | Deduplicar por `ticket_id` + `linea_num` |
| E2 | Canal escrito distinto (`Automac`, `AUTO MAC`, `drive`) | A | 5 % | Tabla de mapeo a valores canónicos |
| E3 | Precio con **coma decimal** (`45,50`) | A | 3 % | Normalizar antes de convertir a decimal |
| E4 | `codigo_lealtad` en minúsculas o con espacios | A | 4 % | `upper(trim())` antes del join |
| E5 | Código de lealtad **que no existe** en el CRM | A | 1 % | Cuarentena |
| E6 | `total_ticket_q` no cuadra con la suma de líneas | A | 1 % | Se recalcula desde las líneas; se marca el ticket |
| E7 | Ticket **anulado** (llega una segunda fila con `ANULADA`) | A | 2 % | Se queda el último estado → genera REVERSO (H3) |
| E8 | Evento de la app **duplicado** (entrega "al menos una vez") | B | 3 % | Deduplicar por `order_id`, quedarse con el último |
| E9 | Pedido cancelado después de entregado | B | 2 % | Igual que E7 |
| E10 | Canje en McDelivery **bajo el mínimo** Q50/Q60 (R15) | B | 0.5 % | Se marca como violación de regla |
| E11 | Campos faltantes en el JSON | B | 1 % | Cuarentena si falta una llave; nulo si es opcional |
| E12 | Correo con mayúsculas o espacios | C | 5 % | `lower(trim())` |
| E13 | **Misma persona con dos cuentas** (mismo nombre y teléfono, distinto correo) (H6) | C | 3 % | Se marca como sospecha; es insumo para ML |
| E14 | Teléfono en formatos distintos (`5555-1234`, `+50255551234`) | C | 30 % | Normalizar a E.164 |
| E15 | Clientes de **Honduras** (R20) | C | 2 % | Se excluyen del programa GT |
| E16 | Departamento con y sin tilde (`Quiché` / `Quiche`) | D | 10 % | Normalizar |
| E17 | Correos del programa anterior que **no se registraron** en el nuevo | F | 15 % | Quedan pendientes de migrar |
| E18 | Saldo del programa anterior **negativo** | F | 0.5 % | Cuarentena |

## 4. Tamaño de la réplica en miniatura

| Elemento | Valor | Justificación |
|---|---|---|
| Restaurantes | 30 (de 126) | Suficiente para ver diferencias por departamento |
| Clientes | 5,000 | Cabe en Databricks Free Edition |
| Periodo | **27-ago-2025 → 20-sep-2026** (~13 meses) | Debe pasar de 365 días para que **haya vencimientos reales** (R10) |
| Tickets | ~60,000–80,000 | Del orden de 1 ticket por cliente cada 3 semanas |

## 5. Arquitectura

Ver [diagramas/arquitectura.md](../diagramas/arquitectura.md): la arquitectura actual inferida
y la réplica medallion en Databricks.

---

## Referencias

Las mismas de la [Fase 1](01_entendimiento_negocio.md#referencias). Se agrega:

7. McDonald's Corporation (2023). *McDonald's and Google Cloud announce strategic partnership.* https://corporate.mcdonalds.com/corpmcd/our-stories/article/mcd-announces-targets-development-loyalty-membership-cloud-tech.html
8. Restaurant Technology News (2026). *McDonald's unifies data from nearly 220 million loyalty users.* https://restauranttechnologynews.com/2026/08/mcdonalds-unifies-data-from-nearly-220-million-loyalty-users-as-global-ai-strategy-takes-shape/
