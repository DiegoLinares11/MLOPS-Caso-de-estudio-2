# Fase 1 — Entendimiento del negocio (CRISP-DM)

**Caso:** sistema de puntos **MiMcDonald's Guatemala**
**Objetivo de la fase:** reconstruir las reglas del programa a partir de fuentes públicas, porque
quienes lo diseñaron ya no están en la empresa. Cada regla se convertirá después en una
**validación** del pipeline (capa Silver/Gold) y en un caso de prueba del POC.

---

## 1. Contexto del cliente

| Dato | Valor | Fuente |
|---|---|---|
| Operador en Guatemala | **McDonald's Mesoamérica** (franquicia maestra de la familia Cofiño desde 1974; también opera El Salvador, Honduras y Nicaragua) | [4], [5] |
| Restaurantes en Guatemala | 126 (2026) | [4] |
| Programa actual | **MiMcDonald's**, lanzado el **27 de agosto de 2025** dentro de la app McDonald's Guatemala | [2], [3] |
| Programa anterior (legado) | **Puntos McDelivery**: 1 punto por Q1, solo en la app de McDelivery | [6], [1] |
| Plataforma digital | Los términos de GT y HN se publican en el mismo dominio (`mcdonaldsdigital.com/GML/...`) → plataforma digital **compartida entre países** | [1] |

## 2. Reglas del programa MiMcDonald's

Todas las reglas salen de los Términos y Condiciones oficiales [1], salvo que se indique otra fuente.

| ID | Regla | Valor exacto | Implicación para los datos |
|---|---|---|---|
| R1 | Tasa de acumulación | **10 puntos por cada Q1.00** gastado | Necesitamos el monto pagado por transacción |
| R2 | Redondeo | Fracciones de quetzal generan puntos proporcionales, **redondeados hacia arriba**; siempre enteros | `ceil(monto × 10)` por transacción |
| R3 | Base de cálculo | Los precios **incluyen IVA (12%)** | A diferencia de EE. UU. (antes de impuestos), aquí el IVA parece sumar puntos → ver hallazgo H2 |
| R4 | Donaciones | No generan puntos | Hay que identificar líneas de donación → se necesita **detalle por línea**, no solo total del ticket |
| R5 | Productos canjeados | No generan puntos nuevos | Otra razón para tener detalle por línea |
| R6 | Canales incluidos | AutoMac, Mostrador, Pantallas digitales (kioscos), McCafé, Centros de Postre y McDelivery (app) | 6 canales con orígenes técnicos distintos |
| R7 | Canales excluidos | Call Center, WhatsApp y apps de terceros (Uber Eats, PedidosYa, etc.) | Filtro en Silver |
| R8 | Tope diario de acumulación | **Máximo 1,000 puntos por día**, sin importar el monto | Equivale a Q100/día; hay que definir cómo se corta (ver H5) |
| R9 | Acreditación | Automática, **máximo 24 horas** después | Sugiere procesamiento **batch** (ver H4) |
| R10 | Vigencia | Los puntos vencen **365 días** después de acumularse | Hay que guardar **cada acumulación con su fecha** → modelo de **ledger por lotes** |
| R11 | Aviso de vencimiento | 30 días antes, el cliente ve en el historial la fecha y los puntos por vencer | Tabla Gold de "puntos por vencer" |
| R12 | Tope diario de canje | **Máximo 15,000 puntos por día** | Validación en el motor de canje |
| R13 | Ofertas por pedido | **Máximo 3 ofertas canjeadas por pedido** | Validación en el motor de canje |
| R14 | Canje irreversible | Los puntos usados no se devuelven, **aunque se cancele la orden** | Un canje es un movimiento definitivo |
| R15 | Mínimo de compra en McDelivery para canjear | Desayuno **Q50.00**; Almuerzo y Cena **Q60.00** | Depende de la hora → hace falta la franja horaria |
| R16 | Catálogo | Recompensas desde **3,000 puntos** (papas medianas, nuggets) hasta **7,500** (Big Tasty, desayuno deluxe); el catálogo cambia sin previo aviso [2] | Catálogo **versionado** (dimensión con historial) |
| R17 | Bienvenida | Quesoburguesa al registrarse en la app y **1,000 puntos** después de la primera compra [2] | Movimiento especial de tipo BIENVENIDA |
| R18 | Cuentas | Una cuenta por correo; nombre, apellido y correo son obligatorios, el teléfono es opcional | Llave natural del cliente = correo |
| R19 | No transferible | Los puntos no pasan de una cuenta a otra | — |
| R20 | Territorio | Solo compras **en Guatemala** acumulan, y solo se canjean en Guatemala | Filtro por país (la plataforma es compartida con HN) |
| R21 | Migración | Los puntos de Puntos McDelivery "se convertirán automáticamente" al nuevo programa | Movimiento de tipo MIGRACION → ver H1 |
| R22 | Fraude | McDonald's puede anular cuentas por "ganancia excesiva de puntos" | Oportunidad para un modelo de ML (Fase 6) |

## 3. Hallazgos preliminares (vacíos y riesgos en las reglas)

Estos son los "huecos" de documentación que un consultor debe reportar. Algunos se resuelven
con un **supuesto explícito** en nuestra réplica.

| ID | Hallazgo | Por qué importa | Supuesto para la réplica |
|---|---|---|---|
| H1 | **La tasa de conversión del programa legado no está publicada.** Puntos McDelivery daba 1 pt/Q1 y MiMcDonald's da 10 pts/Q1. Si se convirtió 1:1, los clientes antiguos perdieron el 90 % del valor. | Riesgo reputacional y de conciliación contable | Conversión ×10 (mismo valor en quetzales) |
| H2 | **Base de cálculo con IVA.** No se dice explícitamente si se suma sobre el total con IVA o sobre el subtotal. | Cambia un 12 % el pasivo en puntos | Total pagado con IVA (así lo sugiere "por cada quetzal gastado") |
| H3 | **No se dice qué pasa con los puntos ganados en una orden cancelada o devuelta.** Solo se cubre el caso inverso (R14). | Posible fraude: comprar, acumular y cancelar | El ledger incluye movimientos de REVERSO |
| H4 | **La acreditación en ≤24 h indica procesamiento batch** (carga nocturna), no tiempo real. | Justifica una arquitectura medallion batch | Pipeline diario |
| H5 | **El tope diario de 1,000 puntos no define el "día" ni el orden de corte** (zona horaria, qué transacción se recorta primero). | Resultados distintos según la implementación | Día calendario en `America/Guatemala`, orden cronológico |
| H6 | **"Dos correos = dos cuentas"** y el teléfono es opcional: no hay forma de detectar a una misma persona con varias cuentas. | Abuso de la bienvenida (quesoburguesa + 1,000 pts) | Generaremos un % de cuentas duplicadas para detectarlas |
| H7 | **Economía del programa:** la recompensa más barata (3,000 pts) exige Q300 de consumo y, por el tope diario, **al menos 3 días distintos** de compra. | Afecta la percepción de valor del programa | Se analizará en Gold con precios del menú |

## 4. Qué nos llevamos a la Fase 2

Las reglas definen las **entidades mínimas** del sistema:

- **Cliente** (llave: correo) — con fecha de registro y bandera de migrado del programa legado
- **Restaurante** — 126 en GT, con departamento
- **Canal** — 6 incluidos + excluidos (para poder filtrarlos)
- **Transacción** (cabecera) y **línea de transacción** (producto, donación, canje)
- **Catálogo de recompensas** — versionado en el tiempo
- **Movimiento de puntos (ledger)** — tipos: `ACUMULACION`, `BIENVENIDA`, `MIGRACION`, `CANJE`, `REVERSO`, `VENCIMIENTO`

La decisión central: **el saldo de puntos no se guarda, se calcula** a partir del ledger. Es el
mismo principio de la contabilidad de doble partida, y es lo que permite cumplir R10/R11
(vencimiento por lote) y auditar cualquier saldo.

---

## Referencias

1. McDonald's Guatemala. *Términos y Condiciones App de McDonald's® en Guatemala.* https://mcdonaldsdigital.com/GML/public/app/gt/terminos-y-condiciones
2. McDonald's Guatemala. *MiMcDonald's Guatemala.* https://www.mi.mcdonalds.com.gt/
3. Periódico Digital Centroamericano y del Caribe (2025). *McDonald's lanza "MiMcDonald's", su nuevo programa de lealtad.* https://newsinamerica.com/pdcc/noticias/gerenciales/2025/mcdonalds-lanza-mimcdonalds-su-nuevo-programa-de-lealtad-para-recompensar-a-sus-invitados/
4. Perspectiva (2026). *McDonald's alcanza 126 restaurantes en Guatemala.* https://perspectiva.gt/empresa/mcdonalds-alcanza-126-restaurantes-guatemala-2026/
5. L'Express Franchise (2026). *McDonald's llega a 126 restaurantes en Guatemala: quién es el dueño.* https://lexpress-franchise.com/latam/ultimas-noticias/mcdonalds-llega-126-restaurantes-guatemala-quien-es-dueno/
6. McDonald's Guatemala. *Puntos McDelivery.* https://puntosmcdelivery.mcdonalds.com.gt/
