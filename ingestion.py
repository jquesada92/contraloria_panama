# -*- coding: utf-8 -*-
"""
Contraloría Panamá Ingestion Script

This script:
1) Checks the last data update date from the Contraloría website.
2) If there is a newer update, it downloads the Excel report for each
   institution and each employment status ("estado").
3) Cleans and normalizes the data and writes it to Parquet files in a staging folder.

NOTE:
- SSL verification is disabled via `verify=False` to match the original code.
  This may raise security warnings and is not recommended for production.
"""

import warnings
warnings.filterwarnings("ignore")  # Keep as in original code (suppresses warnings)

from datetime import datetime as dt
from glob import glob
from re import search
from urllib.parse import quote
import io
from pathlib import Path

from pandas import read_excel, options, to_datetime, concat, DataFrame, read_csv
options.mode.chained_assignment = None  # Keep pandas chained assignment warnings off, as in original

from requests import post, get
from bs4 import BeautifulSoup

print("Ingestion Process")

# =========================
# Configuration
# =========================
# Base folder for outputs. Uses Windows path; adjust as needed.
SOURCE_PATH = r'G:\My Drive\Colab Notebooks\contraloria_panama'

# Staging directory for Parquet outputs; auto-created if not exists.
STAGING_DIR = Path(SOURCE_PATH) / "staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

# Employment statuses ("estados") to iterate over for each institution.
ESTADOS = [
    "EVENTUAL",
    "INTERINO ABIERTO",
    "INTERINO HASTA FIN DE AÑO",
    "PERIODO PROB. 2 AÑOS",
    "PERIODO PROB. DE 1 AÑO",
    "PERMANENTE",
    "POR ASIGNAR",
]

# =========================
# Decorator: validate last update date
# =========================
def validate_last_update_date(func):
    """
    Decorator that checks the last available update date on the Contraloría website
    before executing the wrapped ingestion function.

    Behavior:
    - Reads the update date from the site and compares it with a local file (`last_updated_date.txt`).
    - If the website date is newer (or the file is missing), it runs the ingestion function.
    - Then it persists the new date in the local file.
    - If there is no newer date, it prints 'no updates available' and skips ingestion.

    NOTE: Uses `verify=False` to mirror the original code (disables SSL verification).
    """
    print("Checking for updates")

    def wrapper(*args, **kwargs):
        # Inner helper to fetch the textual update date from the HTML body
        def read_update_date_from_url():
            pattern = r'Fecha de actualización de los datos:\s+(\d{1,2}/\d{1,2}/\d+\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w+)'
            response = get(
                'https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index',
                verify=False  # keep original behavior
            ).text
            extract_date = search(pattern, response)
            return extract_date.group(1)

        # Local file where we store last processed update date
        file = Path('last_updated_date.txt')

        # Parse website-provided date
        webpage_date = dt.strptime(read_update_date_from_url(), '%d/%m/%Y %H:%M:%S %p')

        try:
            # If file exists, read previously stored date
            last_text = file.read_text(encoding='utf-8')
            print(last_text)  # keep original debug print
            last_date = dt.strptime(last_text.strip().lstrip('\x00'), '%Y-%m-%d %H:%M:%S')

            # Only run ingestion if website date is newer
            if last_date < webpage_date:
                func(*args, **kwargs)
                file.write_text(str(webpage_date).lstrip('\x00'), encoding='utf-8')
            else:
                print('no updates available')

        except FileNotFoundError:
            # First-time run: execute ingestion and write the date
            func(*args, **kwargs)
            file.write_text(str(webpage_date), encoding='utf-8')

    return wrapper

# =========================
# Main process
# =========================
@validate_last_update_date
def execute_ingestion():
    """
    Main ingestion entrypoint.

    Steps:
    1) Build the institutions list from the Contraloría website (and persist it as CSV).
    2) For each institution and for each estado, download the Excel report,
       clean the data, and write a Parquet file under the staging folder.
    3) On failure, (institution, estado) is recorded for a second pass retry.
    """
    print('starting update')

    # Collects (institution, estado) pairs that failed, to retry later
    errors: list[tuple[str, str]] = []

    def list_of_institutions() -> list[str]:
        """
        Scrapes the institutions list from the Contraloría page and merges it with
        any previously stored list in 'lista_instituciones.csv' to avoid loss of entries.

        Returns:
            list[str]: A list of institution names.
        """
        resp = post(
            'https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index',
            verify=False  # keep original behavior
        )
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Find the select element that contains the institutions
        select = soup.find("select", {"id": "MainContent_ddlInstituciones"})
        raw_items = select.text.split('\n') if select else []

        # Filter out empty and placeholder rows
        list_inst = [x for x in raw_items if x and x.strip() != "-- Seleccione una institución --"]

        # Persist the institutions list (merge + dedupe with any existing file)
        file_instituciones = Path('lista_instituciones.csv')
        data = DataFrame({'institucion': list_inst})
        if file_instituciones.exists():
            sink_df = read_csv(file_instituciones)
            data = concat([data, sink_df]).drop_duplicates()
        data.to_csv(file_instituciones, index=False)

        return list_inst

    def get_source_data(INSTITUCION: str, ESTADO: str) -> None:
        """
        Downloads one Excel report for a given institution and estado, cleans it,
        and writes the result to a Parquet file under the staging directory.

        Args:
            INSTITUCION (str): Institution name as shown by the website.
            ESTADO (str): Employment status to filter the report by.
        """
        try:
            fecha_consulta = dt.now()

            # Build the report URL with institution and estado filters
            url = (
                'https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Reporte'
                f'?&Ne={quote(INSTITUCION)}&N=&A=&C=&E={quote(ESTADO)}'
            )

            # Request the file (original code uses verify=False)
            resp = get(url, verify=False)

            # Derive a file name from the HTTP header if available
            dispo = resp.headers.get('content-disposition', '') or resp.headers.get('Content-Disposition', '')
            m = search(r'filename="(.*?).xlsx"', dispo)
            file_name = m.group(1) if m else 'reporte_contraloria'

            # Read the Excel content from memory (may fail if server returned HTML)
            bytes_file_obj = io.BytesIO(resp.content)

            # Load all sheets into a list; original logic expects headers in row 4
            sheets = list(read_excel(bytes_file_obj, engine='openpyxl', sheet_name=None).values())

            # Cleans each sheet: drop top 4 rows and set row 3 as header
            def clean_data(df_):
                df = df_.iloc[4:].copy()
                df.columns = df_.iloc[3]
                return df

            # Extract some header cells for date parsing (kept for compatibility)
            # informe = sheets[0].iloc[0, 0]
            fecha_act = sheets[0].iloc[1, 4]
            # institucion_meta = sheets[0].iloc[1, 0]

            # Concatenate all sheets after cleaning
            df = concat(list(map(clean_data, sheets)))

            # Cast numeric columns
            df['Salario'] = df['Salario'].astype('float')
            df['Gasto'] = df['Gasto'].astype('float')

            # Parse the update datetime found on the sheet header (keeps original regex/format)
            df['Fecha Actualizacion'] = str(
                dt.strptime(
                    search(
                        r'.*?:\s+(\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w{2})',
                        str(fecha_act)
                    ).group(1),
                    '%d/%m/%Y %H:%M:%S %p'
                )
            )

            # Add audit/metadata columns
            df['Fecha Consulta'] = fecha_consulta
            df['archivo'] = file_name
            df['Institucion'] = INSTITUCION

            # Normalize column names (lowercase and snake-case)
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]

            # Normalize cedula column name if it has an accent
            if 'cédula' in df.columns:
                df = df.rename(columns={'cédula': 'cedula'})

            # Parse date-like columns to proper types (keeps original formats)
            df['fecha_de_inicio'] = to_datetime(df['fecha_de_inicio'], format="%d/%m/%Y").dt.date
            df['fecha_actualizacion'] = to_datetime(df['fecha_actualizacion'])
            df['fecha_consulta'] = to_datetime(df['fecha_consulta'])

            # Write to Parquet in staging; include estado and a timestamp for uniqueness
            ts = str(fecha_consulta.timestamp()).replace('.', '_')
            out = STAGING_DIR / f"{file_name}_{ESTADO}_{ts}.parquet"
            df.to_parquet(out, index=False, use_deprecated_int96_timestamps=True)

            print("OK ->", INSTITUCION, ESTADO)

        except Exception as e:
            # Record the failing (institution, estado) pair for a retry pass
            errors.append((INSTITUCION, ESTADO))
            print("ERROR ->", INSTITUCION, ESTADO, e)

    def _run(insts: list[str]) -> None:
        """
        Iterates the Cartesian product of institutions × estados,
        calling the downloader for each pair.
        """
        for _institucion in insts:
            for _estado in ESTADOS:
                get_source_data(_institucion, _estado)

    # 1) Build institutions list and run the first pass
    instituciones = list_of_institutions()
    _run(instituciones)

    # 2) If any (institution, estado) failed, retry once
    if errors:
        print("Retrying:", errors)
        retry = errors.copy()
        errors.clear()
        for inst, est in retry:
            get_source_data(inst, est)
        if errors:
            print("Still failing after retry:", errors)

# =========================
# Entrypoint
# =========================
execute_ingestion()
