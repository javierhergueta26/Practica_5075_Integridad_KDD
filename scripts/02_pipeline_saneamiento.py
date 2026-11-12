#!/usr/bin/env python3
"""
Fases 2 y 3: Pipeline de Saneamiento, Coherencia, Detección de Outliers y Perfilado JSON.
Módulo: Big Data Aplicado - Práctica 5075 (RA3, RA5 / CE 3.a, 3.c, 3.d, 5.a, 5.b)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
STAGING_DIR = BASE_DIR / "staging" / "verified"
QUARANTINE_DIR = BASE_DIR / "quarantine"
QUARANTINE_TAMPERED_DIR = QUARANTINE_DIR / "tampered"
BAD_RECORDS_PATH = QUARANTINE_DIR / "bad_records.csv"
PROCESSED_PATH = BASE_DIR / "processed" / "dataset_kdd_listo.csv"
LOG_PATH = BASE_DIR / "logs" / "ejecucion_pipeline.log"
METRICS_JSON_PATH = BASE_DIR / "output_samples" / "metricas_perfilado.json"
QUARANTINE_MANIFEST_PATH = BASE_DIR / "output_samples" / "quarantine_manifest.txt"

ANIO_CURSO = 2026
FECHA_CORTE = pd.Timestamp("2026-11-12 23:59:59")


def configurar_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("Fase2_3_Saneamiento_KDD")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        fh = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


def main():
    logger = configurar_logger()
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger.info("=== INICIO FASE 2: SANEAMIENTO, COHERENCIA Y DETECCIÓN DE OUTLIERS ===")

    ficheros_verificados = sorted(STAGING_DIR.glob("*.csv"))
    ficheros_tampered = sorted(QUARANTINE_TAMPERED_DIR.glob("*.csv"))

    if not ficheros_verificados:
        logger.error("No hay ficheros en staging/verified/. Ejecuta antes 01_verificar_integridad.py")
        return

    dfs = []
    for f in ficheros_verificados:
        df_temp = pd.read_csv(f)
        df_temp["fichero_origen"] = f.name
        dfs.append(df_temp)
        logger.info(f"Cargado fichero verificado: {f.name} ({len(df_temp)} registros)")

    df_raw = pd.concat(dfs, ignore_index=True)
    total_registros_entrada = len(df_raw)
    df_datos_sin_meta = df_raw.drop(columns=["fichero_origen"])

    total_celdas_entrada = df_datos_sin_meta.size
    celdas_nulas_entrada = int(df_datos_sin_meta.isna().sum().sum())
    registros_sin_nulos_entrada = int((~df_datos_sin_meta.isna().any(axis=1)).sum())

    tasa_completitud_inicial_reg = (registros_sin_nulos_entrada / total_registros_entrada) * 100
    tasa_completitud_inicial_celdas = ((total_celdas_entrada - celdas_nulas_entrada) / total_celdas_entrada) * 100

    # Cálculo de umbrales IQR sobre importes positivos para detectar outliers
    importes_positivos = df_raw[df_raw["importe_eur"].notna() & (df_raw["importe_eur"] > 0)]["importe_eur"]
    q1_raw = float(importes_positivos.quantile(0.25))
    q3_raw = float(importes_positivos.quantile(0.75))
    iqr_raw = q3_raw - q1_raw
    limite_superior_iqr = q3_raw + 1.5 * iqr_raw
    limite_inferior_iqr = max(0.01, q1_raw - 1.5 * iqr_raw)

    logger.info(
        f"Umbrales IQR para importe_eur: Q1={q1_raw:.2f} €, Q3={q3_raw:.2f} €, "
        f"IQR={iqr_raw:.2f} €, Límite Superior={limite_superior_iqr:.2f} €"
    )

    moda_categoria = str(df_raw["categoria"].mode()[0])
    moda_metodo_pago = str(df_raw["metodo_pago"].mode()[0])

    registros_limpios = []
    registros_cuarentena = []
    ids_vistos = set()
    imputaciones_realizadas = 0

    for _, row in df_raw.iterrows():
        motivos = []

        # 1. Validación de columnas obligatorias
        id_tx = row["id_transaccion"]
        if pd.isna(id_tx) or str(id_tx).strip() == "":
            motivos.append("NULO_COLUMNA_OBLIGATORIA:id_transaccion")
        elif str(id_tx).strip() in ids_vistos:
            motivos.append("DUPLICADO_ID_TRANSACCION")

        if pd.isna(row["timestamp"]) or str(row["timestamp"]).strip() == "":
            motivos.append("NULO_COLUMNA_OBLIGATORIA:timestamp")

        if pd.isna(row["importe_eur"]):
            motivos.append("NULO_COLUMNA_OBLIGATORIA:importe_eur")

        if pd.isna(row["unidades"]):
            motivos.append("NULO_COLUMNA_OBLIGATORIA:unidades")

        # 2. Coherencia temporal ISO 8601 y año en curso (2026)
        ts_parsed = pd.to_datetime(row["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        if pd.notna(row["timestamp"]) and pd.isna(ts_parsed):
            motivos.append("TIMESTAMP_FORMATO_CORRUPTO")
        elif pd.notna(ts_parsed):
            if ts_parsed.year != ANIO_CURSO:
                motivos.append(f"TIMESTAMP_FUERA_ANIO_CURSO:{ts_parsed.year}")
            elif ts_parsed > FECHA_CORTE:
                motivos.append("TIMESTAMP_FECHA_FUTURA")

        # 3. Reglas de dominio: importe_eur > 0 y unidades >= 1
        if pd.notna(row["importe_eur"]) and float(row["importe_eur"]) <= 0:
            motivos.append(f"DOMINIO_IMPORTE_NO_POSITIVO:{row['importe_eur']}")

        if pd.notna(row["unidades"]) and float(row["unidades"]) < 1:
            motivos.append(f"DOMINIO_UNIDADES_INVALIDAS:{row['unidades']}")

        # 4. Outliers estadísticos (IQR)
        if pd.notna(row["importe_eur"]) and float(row["importe_eur"]) > 0:
            val_imp = float(row["importe_eur"])
            if val_imp > limite_superior_iqr or val_imp < limite_inferior_iqr:
                motivos.append(f"OUTLIER_ESTADISTICO_IQR:{val_imp:.2f}_EUR")

        if motivos:
            fila_bad = row.copy()
            fila_bad["motivo_rechazo"] = " | ".join(motivos)
            registros_cuarentena.append(fila_bad)
            logger.warning(
                f"[CUARENTENA] Segregado (ID={row['id_transaccion']}, Origen={row['fichero_origen']}) "
                f"-> {fila_bad['motivo_rechazo']}"
            )
        else:
            id_limpio = str(id_tx).strip()
            ids_vistos.add(id_limpio)

            fila_ok = row.copy()
            fila_ok["id_transaccion"] = id_limpio
            fila_ok["timestamp"] = ts_parsed.strftime("%Y-%m-%d %H:%M:%S")
            fila_ok["importe_eur"] = round(float(fila_ok["importe_eur"]), 2)
            fila_ok["unidades"] = int(fila_ok["unidades"])

            # Imputación por moda en variables secundarias
            if pd.isna(fila_ok["categoria"]) or str(fila_ok["categoria"]).strip() == "":
                fila_ok["categoria"] = moda_categoria
                imputaciones_realizadas += 1
                logger.info(f"[IMPUTACIÓN] {id_limpio}: 'categoria' imputada con moda ('{moda_categoria}')")

            if pd.isna(fila_ok["metodo_pago"]) or str(fila_ok["metodo_pago"]).strip() == "":
                fila_ok["metodo_pago"] = moda_metodo_pago
                imputaciones_realizadas += 1
                logger.info(f"[IMPUTACIÓN] {id_limpio}: 'metodo_pago' nulo imputado con moda ('{moda_metodo_pago}')")

            registros_limpios.append(fila_ok)

    df_clean = pd.DataFrame(registros_limpios).drop(columns=["fichero_origen"])
    df_bad = pd.DataFrame(registros_cuarentena)

    df_clean.to_csv(PROCESSED_PATH, index=False, encoding="utf-8")
    df_bad.to_csv(BAD_RECORDS_PATH, index=False, encoding="utf-8")
    logger.info(f"Dataset KDD exportado en: {PROCESSED_PATH} ({len(df_clean)} registros válidos)")
    logger.info(f"Registros anómalos exportados en: {BAD_RECORDS_PATH} ({len(df_bad)} registros)")

    # Fase 3: Cálculo de métricas ISO/IEC 25012 y exportación a JSON
    logger.info("=== INICIO FASE 3: PERFILADO KDD (ISO/IEC 25012) ===")

    total_archivos = len(ficheros_verificados) + len(ficheros_tampered)
    tasa_integridad_cripto = (len(ficheros_verificados) / total_archivos) * 100
    tasa_validez_registros = (len(df_clean) / total_registros_entrada) * 100
    tasa_completitud_final = float((~df_clean.isna().any(axis=1)).sum() / len(df_clean) * 100)
    tasa_unicidad_final = float(df_clean["id_transaccion"].nunique() / len(df_clean) * 100)

    stats_importe = {
        "media_eur": round(float(df_clean["importe_eur"].mean()), 2),
        "mediana_eur": round(float(df_clean["importe_eur"].median()), 2),
        "desviacion_estandar_eur": round(float(df_clean["importe_eur"].std()), 2),
        "percentil_25_q1_eur": round(float(df_clean["importe_eur"].quantile(0.25)), 2),
        "percentil_75_q3_eur": round(float(df_clean["importe_eur"].quantile(0.75)), 2),
        "minimo_eur": round(float(df_clean["importe_eur"].min()), 2),
        "maximo_eur": round(float(df_clean["importe_eur"].max()), 2),
    }

    metricas = {
        "proyecto": "Practica_5075_Integridad_KDD",
        "estandar_calidad": "ISO/IEC 25012",
        "fecha_generacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "fase_1_integridad_criptografica": {
            "total_archivos_lote": total_archivos,
            "archivos_validados": len(ficheros_verificados),
            "archivos_rechazados": len(ficheros_tampered),
            "tasa_integridad_criptografica_pct": round(tasa_integridad_cripto, 2),
            "detalle_validados": [f.name for f in ficheros_verificados],
            "detalle_rechazados": [f.name for f in ficheros_tampered],
        },
        "fase_2_saneamiento_y_cuarentena": {
            "registros_brutos_evaluados": total_registros_entrada,
            "registros_conformes_kdd": len(df_clean),
            "registros_segregados_cuarentena": len(df_bad),
            "imputaciones_variables_secundarias": imputaciones_realizadas,
            "regla_imputacion_categoria": f"Moda ({moda_categoria})",
            "regla_imputacion_metodo_pago": f"Moda ({moda_metodo_pago})",
            "umbral_outlier_iqr_superior_eur": round(limite_superior_iqr, 2),
        },
        "fase_3_metricas_iso_25012": {
            "tasa_completitud_inicial_registros_pct": round(tasa_completitud_inicial_reg, 2),
            "tasa_completitud_inicial_celdas_pct": round(tasa_completitud_inicial_celdas, 2),
            "tasa_completitud_final_pct": round(tasa_completitud_final, 2),
            "tasa_unicidad_id_transaccion_pct": round(tasa_unicidad_final, 2),
            "tasa_validez_lote_pct": round(tasa_validez_registros, 2),
        },
        "distribucion_importe_eur_post_limpieza": stats_importe,
    }

    with open(METRICS_JSON_PATH, "w", encoding="utf-8") as jf:
        json.dump(metricas, jf, indent=4, ensure_ascii=False)
    logger.info(f"Métricas JSON guardadas en: {METRICS_JSON_PATH}")

    with open(QUARANTINE_MANIFEST_PATH, "a", encoding="utf-8") as qm:
        qm.write("[FASE 2 - REGISTROS SEGREGADOS A quarantine/bad_records.csv]\n")
        qm.write(f"Total registros aislados: {len(df_bad)} de {total_registros_entrada}\n")
        qm.write("-" * 72 + "\n")
        for _, bad in df_bad.iterrows():
            qm.write(
                f"ID: {bad['id_transaccion']} | Origen: {bad['fichero_origen']} | "
                f"TS: {bad['timestamp']} | Importe: {bad['importe_eur']} | "
                f"Unidades: {bad['unidades']} -> {bad['motivo_rechazo']}\n"
            )

    logger.info("=== FIN PIPELINE DE SANEAMIENTO Y PERFILADO KDD ===")


if __name__ == "__main__":
    main()