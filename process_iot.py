#!/usr/bin/env python3

from __future__ import annotations

import csv
import ctypes
import json
import platform
import sys
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

LIBNAME = "libstats.dylib" if platform.system() == "Darwin" else "libstats.so"
LIBPATH = Path(__file__).with_name(LIBNAME)
lib = ctypes.cdll.LoadLibrary(str(LIBPATH))


class SensorSeries(ctypes.Structure):
    _fields_ = [
        ("values", ctypes.POINTER(ctypes.c_double)),
        ("n", ctypes.c_uint32),
        ("min", ctypes.c_double),
        ("max", ctypes.c_double),
        ("mean", ctypes.c_double),
    ]


lib.compute_stats_batch.argtypes = [ctypes.POINTER(SensorSeries), ctypes.c_size_t]
lib.compute_stats_batch.restype = None

SENSORS: dict[str, str] = {
    "temperatura": "temperatura",
    "umidade": "umidade",
    "luminosidade": "luminosidade",
    "ruido": "ruido",
    "eco2": "eco2",
    "etvoc": "etvoc",
}
VARIABLE_MAP: dict[str, str] = {
    "temperature": "temperatura",
    "humidity": "umidade",
    "luminosity": "luminosidade",
    "noise": "ruido",
    "eco2": "eco2",
    "etvoc": "etvoc",
}
START_DATE = pd.Timestamp(2024, 3, 1, tz="UTC")


def parse_json_rows(json_strings: pd.Series) -> pd.DataFrame:
    rows: list[dict] = []
    for payload in json_strings.dropna():
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            continue
        device = obj.get("device_name") or obj.get("device_id") or "unknown"
        for entry in obj.get("data", []):
            var = entry.get("variable")
            col = VARIABLE_MAP.get(var)
            if not col:
                continue
            raw = str(entry.get("value", "")).strip().lstrip("+").replace(",", ".")
            try:
                value = float(raw)
            except ValueError:
                continue
            ts = pd.to_datetime(entry.get("time"), errors="coerce", utc=True)
            if pd.isna(ts):
                continue
            rows.append({"device": device, "data": ts, col: value})
    return pd.DataFrame(rows)


def main(infile: str, outfile: str) -> None:
    with open(infile, "r", newline="") as fh:
        sample = fh.readline()
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
    df_raw = pd.read_csv(infile, sep=dialect.delimiter, dtype=str)

    mask_json = df_raw["device"].str.startswith("{", na=False)
    df_json = parse_json_rows(df_raw.loc[mask_json, "device"])
    df_csv = df_raw.loc[~mask_json].copy()

    df_csv["data"] = pd.to_datetime(df_csv["data"], errors="coerce", utc=True)
    df_csv = df_csv[df_csv["data"] >= START_DATE]
    if not df_json.empty:
        df_json = df_json[df_json["data"] >= START_DATE]

    for col in SENSORS.values():
        if col not in df_csv.columns:
            df_csv[col] = np.nan
        df_csv[col] = (
            df_csv[col]
            .str.replace(",", ".", regex=False)
            .str.strip()
            .replace({"": np.nan, "NULL": np.nan, "-": np.nan})
            .astype(float)
        )

    df = pd.concat([df_csv, df_json], ignore_index=True, sort=False)
    df.sort_values(by="data", inplace=True, ignore_index=True)

    df["ano_mes"] = df["data"].dt.strftime("%Y-%m")
    devices = df["device"].dropna().unique()
    months = df["ano_mes"].dropna().unique()
    sensors = list(SENSORS.keys())

    grouped = df.groupby(["device", "ano_mes"], sort=False)

    metadata: list[tuple[str, str, str]] = []
    series_ptrs: list[SensorSeries] = []
    array_refs: list[np.ndarray] = []

    for (dev, ym), frame in grouped:
        for sensor_key, col in SENSORS.items():
            vals = frame[col].dropna().to_numpy(dtype=np.float64)
            metadata.append((dev, ym, sensor_key))
            if vals.size == 0:
                series_ptrs.append(SensorSeries(None, 0, 0.0, 0.0, 0.0))
            else:
                arr = np.ascontiguousarray(vals)
                array_refs.append(arr)
                series_ptrs.append(
                    SensorSeries(
                        arr.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
                        arr.size,
                        0.0,
                        0.0,
                        0.0,
                    )
                )

    if series_ptrs:
        SeriesArray = SensorSeries * len(series_ptrs)
        c_array = SeriesArray(*series_ptrs)
        lib.compute_stats_batch(c_array, len(series_ptrs))
    else:
        c_array = []

    rows: list[dict[str, object]] = []
    for i, (dev, ym, sensor) in enumerate(metadata):
        s = c_array[i]
        rows.append(
            {
                "device": dev,
                "ano-mes": ym,
                "sensor": sensor,
                "valor_maximo": round(s.max, 3),
                "valor_medio": round(s.mean, 3),
                "valor_minimo": round(s.min, 3),
            }
        )

    existing = {(r["device"], r["ano-mes"], r["sensor"]) for r in rows}
    for dev, ym, sensor in product(devices, months, sensors):
        if (dev, ym, sensor) not in existing:
            rows.append(
                {
                    "device": dev,
                    "ano-mes": ym,
                    "sensor": sensor,
                    "valor_maximo": 0.0,
                    "valor_medio": 0.0,
                    "valor_minimo": 0.0,
                }
            )

    out_df = pd.DataFrame(rows)
    out_df.sort_values(["ano-mes", "device", "sensor"], inplace=True)
    out_df.to_csv(outfile, sep=";", index=False)
    print("Resultado salvo em:", outfile)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Uso: python process_iot.py <entrada.csv> <saida.csv>")
    main(sys.argv[1], sys.argv[2])
