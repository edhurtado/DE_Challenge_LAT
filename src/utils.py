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

async def process_json_chunk(
        json_chunk: List[Dict[str, Any]],
        keys: List[str],
        regex_col: str,
        regex: str):
    
    filtered_json = []
    regex_pattern = re.compile(regex)
    
    for entry in json_chunk:
        filtered_entry = {key: entry.get(key) for key in keys if key in entry}
        if regex_col in entry:
            matches = regex_pattern.findall(entry[regex_col])
            filtered_entry[regex_col] = matches if matches else []
        filtered_json.append(filtered_entry)
    
    return filtered_json

def count_unique_values(
        filtered_json: List[Dict[str, Any]], 
        regex_col: str,
        required_columns:list=None):

    count_json = []

    for entry in filtered_json:
        if regex_col in entry:
            counts = defaultdict()
            for item in entry[regex_col]:
                counts[item] += 1
            if required_columns:
                for key in reversed(required_columns):
                    counts = {entry[key]: counts}
            count_json.append(counts)

    return count_json


async def unify_counts():
    pass

async def process_and_count_json_file(
        keys:List[str], 
        regex_col:str, 
        regex:str, 
        chunk_size:int=5e3):
    global FILE_URL

    json_data = await download_and_load_json_from_drive(FILE_URL)
    
    all_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    overall_counts = defaultdict(int)

    for i in range(0, len(json_data), chunk_size):
        chunk = json_data[i:i + chunk_size]
        filtered_json = await process_json_chunk(chunk, keys, regex_col, regex)
        counts, unique_counts = count_unique_values(filtered_json, regex_col)
