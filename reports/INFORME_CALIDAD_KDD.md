# Informe Técnico de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

**Proyecto:** `Practica_5075_Integridad_KDD`  
**Módulo:** Big Data Aplicado (RD 279/2021 — RA3 y RA5)

---

## 1. Resumen de Métricas de Calidad (ISO/IEC 25012)

| Dimensión / Indicador | Valor Obtenido | Detalle del Lote `raw_batch_202611` |
| :--- | :---: | :--- |
| **Tasa de Integridad Criptográfica (SHA-256)** | **66,67 %** | 2 ficheros válidos (`norte` y `este`) / 1 rechazado (`sur`) |
| **Tasa de Completitud Inicial (Registros)** | **95,00 %** | 95 de 100 registros sin valores nulos en origen |
| **Tasa de Completitud Inicial (Celdas)** | **99,29 %** | 695 de 700 celdas informadas en origen |
| **Tasa de Completitud Final (Dataset KDD)** | **100,00 %** | 87 de 87 registros completos tras saneamiento e imputación |
| **Tasa de Unicidad (`id_transaccion`)** | **100,00 %** | 87 identificadores únicos sin duplicados |
| **Tasa de Validez del Lote** | **87,00 %** | 87 registros aprobados / 13 segregados a cuarentena |

---

## 2. Decisiones Técnicas Adoptadas por Fase

### Fase 1: Pasarela Criptográfica SHA-256 por Lotes
* **Ficheros aprobados (`staging/verified/`):** `transacciones_tienda_norte.csv` y `transacciones_tienda_este.csv` coincidieron exactamente con las firmas oficiales de `manifest_checksums.sha256`.
* **Fichero rechazado (`quarantine/tampered/`):** `transacciones_tienda_sur.csv` presentó una firma SHA-256 discrepante (`1c47dff4...` frente a `0773c2cd...`), detectándose manipulación no autorizada e impidiendo su paso al pipeline de transformación.

### Fase 2: Saneamiento, Coherencia y Detección de Outliers
Sobre los **100 registros** procedentes de las tiendas Norte (50) y Este (50):
1. **Segregación a cuarentena (`quarantine/bad_records.csv` — 13 registros):**
   * **Nulos en columnas obligatorias (3 registros):** 2 filas sin `id_transaccion` y 1 fila sin `importe_eur` (`TXE-20261110-0034`).
   * **Incoherencia temporal ISO 8601 (3 registros):** 1 fecha futura en 2027 (`TXE-20261110-0010`), 1 fecha fuera del año en curso (`2024`, `TXE-20261110-0022`) y 1 cadena corrupta `2026-99-99 INVALID_TS` (`TXE-20261110-0039`).
   * **Reglas de dominio (4 registros):** 2 importes no positivos (`-34,50 €` y `0,00 €`) y 2 cantidades inválidas (`0` y `-3` unidades).
   * **Outliers estadísticos por método IQR (3 registros):** Importes extremos que superan el umbral superior $Q_3 + 1,5 \times IQR = 140,84\text{ €}$ (`1480,00 €`, `2750,90 €` y `4120,00 €`), atribuibles a fallos de lectura de TPV/sensores.
2. **Imputación controlada de variables secundarias (2 registros):**
   * Se imputó `categoria` nula en `TXE-20261110-0007` usando la **moda** (`Textil`) y `metodo_pago` nulo en `TXE-20261110-0015` usando la **moda** (`Transferencia`), evitando descartar transacciones comerciales válidas sin distorsionar las variables económicas.

---

## 3. Perfilado Estadístico Post-Limpieza (`importe_eur`)

Distribución de `importe_eur` en el dataset final `processed/dataset_kdd_listo.csv` ($N = 87$):

| Métrica Estadística | Valor |
| :--- | :---: |
| **Media aritmética** | 70,99 € |
| **Mediana (Percentil 50)** | 73,44 € |
| **Desviación estándar** | 26,34 € |
| **Percentil 25 ($Q_1$)** | 53,80 € |
| **Percentil 75 ($Q_3$)** | 85,34 € |
| **Mínimo – Máximo** | 12,50 € – 124,47 € |

---

## 4. Conclusiones Técnicas para Business Intelligence y KDD

1. **Integridad y no repudio:** La validación SHA-256 evita que ficheros alterados en tránsito (`transacciones_tienda_sur.csv`) contaminen el almacén analítico, cumpliendo la característica de integridad de la norma ISO/IEC 25012.
2. **Aislamiento sin caída del servicio:** La arquitectura de cuarentena (`quarantine/tampered/` y `quarantine/bad_records.csv`) permite auditar y reprocesar incidencias sin bloquear la entrega diaria de datos limpios a BI.
3. **Consistencia para minería de datos (KDD):** Al eliminar importes negativos, fechas fuera de ejercicio y valores atípicos de miles de euros, la media (`70,99 €`) y la mediana (`73,44 €`) convergen, garantizando que los algoritmos posteriores de segmentación (*clustering*), asociación o predicción de ventas no sufran sesgos.