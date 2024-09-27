import requests
import zipfile
import json
import re
import os
import asyncio
import aiofiles
from io import BytesIO
from typing import List, Dict, Any, Tuple, Union
from collections import defaultdict



# 1. Funcion para leer JSON y convertirlos en chunks
# 2. Limpiar con Regex lo que no se desea y separarlos
# 3. Convertir en sets (obtener valores únicos)
# 4. Contar valor especifico.
FILE_URL = "https://drive.google.com/file/d/1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis/"

async def load_json_lines(file_path):
    data = []
    async with aiofiles.open(file_path, mode='r') as file:
        async for line in file:
            data.append(json.loads(line))
    return data

async def download_and_load_json_from_drive(file_URL:str=FILE_URL):
    
    response = requests.get(file_URL)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        json_files = [file for file in zip_file.filelist() if file.endswith(".json")]
        if not json_files:
            raise FileNotFoundError("No se encontraron archivos JSON en el ZIP.")
        
        with open("temp.json", "wb") as temp_file:
            for json_file in json_files:
                with zip_file.open(json_file) as file:
                    temp_file.write(file.read())
    
    json_data = await load_json_lines('temp.json')
    return json_data

async def process_json_chunk():
    pass

async def count_unique_values():
    pass

async def unify_counts():
    pass

async def process_and_count_json_file(keys:List[str], regex_col:str, chunk_size:int=5e3):
    global FILE_URL

    json_data = await download_and_load_json_from_drive(FILE_URL)

