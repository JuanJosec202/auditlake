# Bitácora de uso de IA

Registro de prompts clave, aceleraciones y correcciones humanas. Es la fuente de la sección de IA del README
y de la pregunta 4. Solo casos reales, con evidencia.

## Plantilla

### [AAAA-MM-DD] Fase N: título corto
- **Herramienta:** Claude / Claude Code / Copilot / ChatGPT
- **Prompt:** texto exacto o resumido
- **Respuesta de la IA:** qué propuso
- **Tipo:** acierto / genérica / errónea / ineficiente / fuera de alcance
- **Problema:** por qué no servía tal cual (si aplica)
- **Decisión humana:** qué se hizo en su lugar
- **Evidencia:** commit, test, medición o salida de comando
- **Aceleró:** tiempo o trabajo ahorrado

---

## Entradas

### [2026-11-25] Diseño: revisión de la propuesta de arquitectura
- **Herramienta:** Claude
- **Prompt:** evaluar una propuesta inicial (outbox + PySpark + Iceberg + dbt) contra el enunciado y la rúbrica.
- **Respuesta de la IA:** validó el enfoque y señaló huecos.
- **Tipo:** acierto (con revisión humana del alcance)
- **Decisión humana:** _completar: qué aceptaste, qué recortaste y por qué_
- **Evidencia:** CLAUDE.md (alcance congelado)
- **Aceleró:** _completar_
