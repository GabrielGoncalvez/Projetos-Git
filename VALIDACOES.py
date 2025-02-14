
import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta
from alive_progress import alive_bar
import time
import re
import sys  # Importa o módulo sys para encerrar o script
from openpyxl import load_workbook
import cx_Oracle as oracle
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import numpy as np
import glob
import warnings
from colorama import Fore, Style, init
from sas7bdat import SAS7BDAT

# Cores para o print
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"
try:
    # Criar o DataFrame de mapeamento
    month_map = pd.DataFrame({
        'PERIODO': ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'],
        'MONTH_NUM': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    })

    # Gerar a parte da query SQL para a conversão de PERIODO para DATA_BASE
    case_statements = "CASE "
    for _, row in month_map.iterrows():
        case_statements += f"WHEN substr(PERIODO, 1, 3) = '{row['PERIODO']}' THEN '{row['MONTH_NUM']}' "
    case_statements += "END"

    # Parte da query SQL completa
    data_base_conversion = f"'01' || {case_statements} || '20' || substr(PERIODO, 5, 2) AS DATA_BASE"

    print(data_base_conversion)

except Exception as e:
    print(f"{RED}Erro na padronização de database: {e}")
