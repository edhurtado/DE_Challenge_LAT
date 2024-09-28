import requests
import zipfile
import json
import re
import os
import asyncio
import aiofiles
import gdown
from io import BytesIO
from typing import List, Dict, Any, Tuple, Union
from collections import defaultdict
import pathlib

# 1. Funcion para leer JSON y convertirlos en chunks
# 2. Limpiar con Regex lo que no se desea y separarlos
# 3. Convertir en sets (obtener valores únicos)
# 4. Contar valor especifico.
FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
FILE_ID = "1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
current_path = pathlib.Path(__file__).parent.resolve()
FILE_NAME = os.path.join(current_path, "tweets.json.zip")
#FILE_URL = "https://drive.google.com/uc?id=1LRFKh7e-O7IP3IhEyTWlL8RBhaAapWtv" #test file

def load_json_lines(file_path):
    data = []
    with open(file_path, mode='r') as file:
        for line in file:
            data.append(json.loads(line))
    return data

def download_and_load_json_from_drive(file_url:str=FILE_URL, file_name:str=FILE_NAME):
    
    raw_file = gdown.download(file_url, file_name, quiet=False)

    #with open(raw_file) as file:
    with zipfile.ZipFile(raw_file) as zip_file:
            json_files = [file for file in zip_file.namelist() if file.endswith(".json")]
            if not json_files:
                raise FileNotFoundError("No se encontraron archivos JSON en el ZIP.")

            with open("temp.json", "wb") as temp_file:
                for json_file in json_files:
                    with zip_file.open(json_file) as file:
                        temp_file.write(file.read())
    
    json_data = load_json_lines('temp.json')
    return json_data

def process_json_chunk(
        json_chunk: List[Dict[str, Any]],
        keys: List[str],
        regex_col: str,
        regex: str):
    
    filtered_json = []
    regex_pattern = re.compile(regex or "")
    
    for entry in json_chunk:
        filtered_entry = {}

        # Iteramos sobre las keys para obtener los valores
        for key in keys:
            if isinstance(key, list):
                temp_dict = entry
                for k in key:
                    temp_dict = temp_dict[k]
                    if isinstance(temp_dict, dict):
                        continue
                    else:
                        value = temp_dict
                filtered_entry[k] = value
            else:
                if key in entry:
                    value = entry[key].split("T")[0]
                    filtered_entry[key] = value

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
            counts = dict()
            if isinstance(entry[regex_col],list):
                for item in entry[regex_col]:
                    counts[item] += 1
            else:
                if counts.get(regex_col):
                    counts[entry[regex_col]] += 1
                else:
                    counts[entry[regex_col]] = 1
        if required_columns:
            for key in reversed(required_columns):
                counts = {entry[key]: counts}
        count_json.append(counts)

    return count_json

def get_keys(my_dict:dict, keys:list=None):
    if isinstance(keys, list):
        for key, value in my_dict.items():
            keys.append(key)
            if isinstance(value, dict):
                get_keys(value, keys)
    return keys

def unify_dicts(dict1:dict, dict2:dict, keys):
    def recursive_update(d1, d2, keys):
        if len(keys) == 1:
            if keys[0] in d2:
                if keys[0] not in d1:
                    d1[keys[0]] = 0
            #if keys[0] in d1 and keys[0] in d2:
                d1[keys[0]] += d2[keys[0]]
        else:
            key = keys[0]
            if key in d2:
                if key not in d1:
                    d1[key] = {}
            #if key in d1 and key in d2:
                recursive_update(d1[key], d2[key], keys[1:])
    
    recursive_update(dict1, dict2, keys)
    return dict1

def unify_dicts_recursive(dict1, dict2):
    for key in dict2:
        if key in dict1:
            if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                unify_dicts_recursive(dict1[key], dict2[key])
            else:
                dict1[key] += dict2[key]
        else:
            dict1[key] = dict2[key]
    return dict1

def unify_counts(counts: List[Dict[str, Any]], base_keys: List[str]):
    consolidated = defaultdict()

    for count in counts:
        json_keys = []
        json_keys = get_keys(count, json_keys)
        consolidated = unify_dicts(consolidated, count, json_keys)

    return consolidated

def process_and_count_json_file(
        keys:List[str], 
        regex_col:str, 
        regex:str, 
        chunk_size:int=5e3):
    global FILE_URL

    json_data = download_and_load_json_from_drive(FILE_URL)
    
    all_counts = dict()

    for i in range(0, len(json_data), chunk_size):
        chunk = json_data[i:i + chunk_size]
        filtered_json = process_json_chunk(chunk, keys, regex_col, regex)
        counts = count_unique_values(filtered_json, "username", ["date"])

        # Unificar por chunk
        json_keys = []
        base_json_keys = get_keys(counts[0], json_keys)
        unified_chunk = unify_counts(counts, base_json_keys)
        all_counts = unify_dicts_recursive(all_counts, unified_chunk)
        
    return all_counts

if __name__=="__main__":
    count = process_and_count_json_file(keys=["date", ["user", "username"]], regex_col="username", regex=None, chunk_size=int(5e3))
    print(count)
    #print (current_path, FILE_NAME)