import heapq
import zipfile
import time
import json
import re
import os
import gdown
from typing import List, Dict, Any
from collections import defaultdict
import pathlib

FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
FILE_ID = "1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
current_path = pathlib.Path(__file__).parent.resolve()
FILE_NAME = os.path.join(current_path, "tweets.json.zip")
EMOJI_REGEX = "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002702-\U000027B0\U000024C2-\U0001F251]+"
MENTION_REGEX = r"@(\w+)"

def load_json_lines(file_path):
    data = []
    with open(file_path, mode='r') as file:
        for line in file:
            data.append(json.loads(line))
    return data

def download_and_load_json_from_drive(file_url:str=FILE_URL, file_name:str=FILE_NAME, only_file:bool=False):
    
    raw_file = gdown.download(file_url, file_name, quiet=False)

    with zipfile.ZipFile(raw_file) as zip_file:
            json_files = [file for file in zip_file.namelist() if file.endswith(".json")]
            if not json_files:
                raise FileNotFoundError("No se encontraron archivos JSON en el ZIP.")

            with open("temp.json", "wb") as temp_file:
                for json_file in json_files:
                    with zip_file.open(json_file) as file:
                        temp_file.write(file.read())
    
    if not only_file:
        json_data = load_json_lines("temp.json")
        return json_data

def clean_date(date:str):
    pattern = r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})\+\d{2}:\d{2}'
    match_date = re.match(pattern, date)

    if match_date:
        cleaned_date = match_date.group(1)
        return cleaned_date
    else:
        return date

def process_json_chunk(
        json_chunk: List[Dict[str, Any]],
        keys: List[str],
        regex_col: str,
        regex: str):
    
    filtered_json = []
    alphanumeric_pattern = re.compile(r"^[\w]+$")
    regex_pattern = re.compile(regex or "")
    
    for entry in json_chunk:
        filtered_entry = {}

        # Iterate over keys to get values
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
                    value = clean_date(entry[key])
                    filtered_entry[key] = value

        if regex_col in entry:
            matches = regex_pattern.findall(entry[regex_col])
            if matches:
                if alphanumeric_pattern.match(matches[0]): #If it's a mention don't split
                    filtered_entry[regex_col] = matches
                else: # Only emojis should spplited
                    filtered_entry[regex_col] = flatten_list(matches)
            else:
                filtered_entry[regex_col] = [""]
        filtered_json.append(filtered_entry)
    
    return filtered_json

def flatten_list(my_list):
    if isinstance(my_list, list):
        return [element for sub_list in my_list for element in sub_list]
    return []

def join_str_list(str_list:List[str]):
    return ["".join(str_list)]

def count_unique_values(
        filtered_json: List[Dict[str, Any]], 
        regex_col: str,
        required_columns:list=None):

    count_json = []

    for entry in filtered_json:
        if regex_col in entry:
            counts = dict()
            if entry[regex_col]==[""]:
                continue
            elif isinstance(entry[regex_col],list):
                for item in entry[regex_col]:
                    if counts.get(item):
                        counts[item] +=1
                    else:
                        counts[item] = 1
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

def get_nth_value_from_dict(my_dict:dict, nth:int=2, asc:bool=False):
    if not asc:
        return heapq.nlargest(nth, my_dict.items(), key=lambda x: x[1])
    else:
        return heapq.nsmallest(nth, my_dict.items(), key=lambda x: x[1])


def consolidate_dict(my_dict:dict):
    sum_dict = dict()
    for key in my_dict.keys():
        try:
            total_sum = sum(my_dict[key].values()) if isinstance(my_dict[key], dict) else my_dict[key]
            sum_dict[key] = total_sum
        except:
            sum_dict[key] = None
    return sum_dict

def unify_dicts_on_keys(dict1:dict, dict2:dict, keys):
    def recursive_update(d1, d2, keys):
        if len(keys) == 1:
            if keys[0] in d2:
                if keys[0] not in d1:
                    d1[keys[0]] = 0
                d1[keys[0]] += d2[keys[0]]
        else:
            key = keys[0]
            if key in d2:
                if key not in d1:
                    d1[key] = {}
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

def unify_counts(counts: List[Dict[str, Any]], base_keys: List[str], inner_keys:bool=False):
    consolidated = defaultdict()

    for count in counts:
        if inner_keys:
            json_keys = []
            json_keys = get_keys(count, json_keys)
            consolidated = unify_dicts_on_keys(consolidated, count, json_keys)
        else:
            consolidated = unify_dicts_recursive(consolidated, count)

    return consolidated

def process_and_count_json_file(
        keys:List[str], 
        regex_col:str, 
        regex:str, 
        count_keys:List[str],
        chunk_size:int=5e3,
        inner_keys=True
        ):
    global FILE_URL

    json_data = download_and_load_json_from_drive(FILE_URL)
    
    all_counts = dict()

    for i in range(0, len(json_data), chunk_size):
        chunk = json_data[i:i + chunk_size]
        filtered_json = process_json_chunk(chunk, keys, regex_col, regex)
        counts = count_unique_values(filtered_json, regex_col, count_keys)

        # Unify by chunk
        json_keys = []
        base_json_keys = get_keys(counts[0], json_keys)
        unified_chunk = unify_counts(counts, base_json_keys, inner_keys=inner_keys)
        all_counts = unify_dicts_recursive(all_counts, unified_chunk)
        
    return all_counts

if __name__=="__main__":
    start_time = time.perf_counter()
    count = process_and_count_json_file(keys=["date", ["user", "username"]], regex_col="username", regex=None, count_keys=["date"], chunk_size=int(5e4), inner_keys=True)
    #count = process_and_count_json_file(keys=["content"], regex_col="content", regex=EMOJI_REGEX, count_keys=None, chunk_size=int(5e4), inner_keys=False)
    #count = process_and_count_json_file(keys=["content"], regex_col="content", regex=MENTION_REGEX, count_keys=None, chunk_size=int(5e4), inner_keys=False)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    cons_dict = consolidate_dict(count)
    max_value = get_nth_value_from_dict(cons_dict, 10, False)
    print(cons_dict)
    print(max_value)
    print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
