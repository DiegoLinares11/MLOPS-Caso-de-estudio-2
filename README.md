# MLOPS-Caso-de-estudio-2 — Sistema de puntos MiMcDonald's Guatemala

Curso **Machine Learning Engineering (CC3105)** — Universidad del Valle de Guatemala

Diego Linares · Andy Fuentes · Christian Echeverria · Diederich Solis

Diagnóstico y réplica en miniatura del programa de lealtad **MiMcDonald's** de McDonald's
Guatemala, siguiendo CRISP-DM y una arquitectura medallion (Bronze → Silver → Gold) en
Databricks Free Edition.

## Avance

| Fase CRISP-DM | Entregable | Estado |
|---|---|---|
| 1. Entendimiento del negocio | [docs/01_entendimiento_negocio.md](docs/01_entendimiento_negocio.md) | Listo |
| 2. Entendimiento de los datos | [docs/02_entendimiento_datos.md](docs/02_entendimiento_datos.md) · [diagramas/arquitectura.md](diagramas/arquitectura.md) | Listo |
| 3. Preparación de datos | [docs/03_preparacion_datos.md](docs/03_preparacion_datos.md) · [docs/arquitectura_medallion.md](docs/arquitectura_medallion.md) · notebooks 00, 01 y 02 | En curso |
| 4. Modelado | Propuesta de ML/AI/LLM | Pendiente |
| 5. Evaluación | POC del motor de puntos con pruebas | Pendiente |
| 6. Despliegue | Reporte final y recomendaciones | Pendiente |

## Cómo correrlo en Databricks

1. En el workspace: **Workspace → Create → Git folder** con la URL de este repositorio.
2. Abrir `notebooks/00_generador_datos.py` y conectarlo a **Serverless**.
3. **Run all**. Crea el schema `workspace.mimcdonalds`, los Volumes `landing` y `control`, y
   escribe las 6 fuentes (~800 archivos). Es idempotente: se puede correr las veces que sea.
4. Correr `notebooks/01_bronze.py` de la misma forma: crea las 6 tablas `bronze_*` y verifica
   que tengan las filas esperadas.
5. Correr `notebooks/02_silver.py`: crea las 7 tablas `silver_*` y verifica que se detectaron
   todos los errores inyectados.

También corre en local (`python notebooks/00_generador_datos.py`): escribe en `./_datos_locales/`.

## Estructura

```
MLOPS-Caso-de-estudio-2/
├── docs/        # Documentación por fase y guía de la arquitectura medallion
├── diagramas/   # Arquitectura de datos (Mermaid)
└── notebooks/   # Notebooks de Databricks (formato .py)
    ├── 00_generador_datos.py   # Genera las fuentes sintéticas y la hoja de respuestas
    ├── 01_bronze.py            # Landing → 6 tablas bronze_*, todo como texto
    └── 02_silver.py            # Bronze → 7 tablas silver_*, limpias y unificadas
```
