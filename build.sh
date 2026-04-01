#!/usr/bin/env bash
set -e

echo "==> Creando directorios del data lake..."
mkdir -p data/raw data/lake/bronze data/lake/silver data/lake/gold

echo "==> Generando archivos XLSX sintéticos..."
python data.py

echo "==> Moviendo archivos al directorio raw..."
mv transacciones_*.xlsx data/raw/

echo "==> Ejecutando pipeline Medallion..."
python main.py

echo "==> Build completado."