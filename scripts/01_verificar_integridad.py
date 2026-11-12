#!/usr/bin/env python3
"""
Fase 1: Pasarela de Integridad y Validación Criptográfica por Lotes (SHA-256).
Módulo: Big Data Aplicado - Práctica 5075 (RA3 / CE 3.b)
"""

import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_DIR = BASE_DIR / "staging" / "verified"
QUARANTINE_TAMPERED_DIR = BASE_DIR / "quarantine" / "tampered"
LOG_PATH = BASE_DIR / "logs" / "ejecucion_pipeline.log"
QUARANTINE_MANIFEST_PATH = BASE_DIR / "output_samples" / "quarantine_manifest.txt"


def configurar_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("Fase1_Integridad")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        fh = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


def calcular_sha256(ruta_fichero: Path) -> str:
    hasher = hashlib.sha256()
    with open(ruta_fichero, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def main():
    logger = configurar_logger()
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger.info("=== INICIO FASE 1: PASARELA CRIPTOGRÁFICA SHA-256 ===")

    if not MANIFEST_PATH.exists():
        logger.error(f"No se encuentra el manifiesto oficial en {MANIFEST_PATH}")
        return

    archivos_validados = []
    archivos_rechazados = []

    with open(MANIFEST_PATH, "r", encoding="utf-8") as manifest:
        for num_linea, linea in enumerate(manifest, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue

            partes = linea.split()
            if len(partes) < 2:
                logger.warning(f"Línea {num_linea} mal formada en manifiesto: {linea}")
                continue

            hash_esperado, nombre_archivo = partes[0].lower(), partes[1].strip()
            ruta_origen = RAW_DIR / nombre_archivo

            if not ruta_origen.exists():
                logger.error(f"Fichero no encontrado: {nombre_archivo}")
                archivos_rechazados.append((nombre_archivo, "NO_ENCONTRADO", hash_esperado, "N/A"))
                continue

            hash_calculado = calcular_sha256(ruta_origen).lower()

            if hash_calculado == hash_esperado:
                destino = STAGING_DIR / nombre_archivo
                shutil.move(str(ruta_origen), str(destino))
                archivos_validados.append(nombre_archivo)
                logger.info(f"[OK] {nombre_archivo} -> Firma verificada ({hash_calculado[:12]}...). Movido a staging/verified/")
            else:
                destino = QUARANTINE_TAMPERED_DIR / nombre_archivo
                shutil.move(str(ruta_origen), str(destino))
                archivos_rechazados.append((nombre_archivo, "HASH_SHA256_DISCREPANTE", hash_esperado, hash_calculado))
                logger.critical(
                    f"[ALERTA SEGURIDAD] {nombre_archivo} -> CORRUPTO/MANIPULADO. "
                    f"Esperado: {hash_esperado} | Calculado: {hash_calculado}. Segregado a quarantine/tampered/"
                )

    with open(QUARANTINE_MANIFEST_PATH, "w", encoding="utf-8") as qm:
        qm.write("========================================================================\n")
        qm.write("MANIFIESTO DE CUARENTENA - PIPELINE DE CALIDAD DE DATOS KDD (ISO 25012)\n")
        qm.write(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        qm.write("========================================================================\n\n")
        qm.write("[FASE 1 - FICHEROS RECHAZADOS POR INTEGRIDAD CRIPTOGRÁFICA (SHA-256)]\n")
        if archivos_rechazados:
            for item in archivos_rechazados:
                qm.write(f"- Archivo: {item[0]} | Motivo: {item[1]}\n")
                qm.write(f"  Hash Esperado : {item[2]}\n")
                qm.write(f"  Hash Calculado: {item[3]}\n")
        else:
            qm.write("- Ningún fichero rechazado en Fase 1.\n")
        qm.write("\n")

    total = len(archivos_validados) + len(archivos_rechazados)
    tasa = (len(archivos_validados) / total * 100) if total > 0 else 0.0
    logger.info(f"=== FIN FASE 1 | Validados: {len(archivos_validados)}/{total} ({tasa:.2f}%) | Rechazados: {len(archivos_rechazados)}/{total} ===")


if __name__ == "__main__":
    main()