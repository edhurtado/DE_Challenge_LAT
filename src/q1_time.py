import utils
import time
import datetime
import pathlib
import os
from typing import List, Tuple
from datetime import datetime
from memory_profiler import memory_usage

current_path = pathlib.Path(__file__).parent.resolve()
FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
TEMP_FILE_NAME = os.path.join(current_path, "tweets.json.zip")

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    global FILE_URL, TEMP_FILE_NAME

    # Defining configuration parameters
    keys = ["date", ["user", "username"]]
    regex_col = "username"
    regex = None
    count_keys = ["date"]
    chunk_size = int(5e3)
    inner_keys = True

    if not file_path:
        # Download & Load JSON Zipped file from Google Drive
        json_data = utils.download_and_load_json_from_drive(FILE_URL, TEMP_FILE_NAME)
    else:
        # Load JSON if exists in path
        json_data = utils.load_json_lines(file_path)

    all_counts = dict()
    result = []

    for i in range(0, len(json_data), chunk_size):
        chunk = json_data[i:i + chunk_size]
        filtered_json = utils.process_json_chunk(chunk, keys, regex_col, regex)
        counts = utils.count_unique_values(filtered_json, regex_col, count_keys)

        # Unify dict by chunk
        json_keys = []
        base_json_keys = utils.get_keys(counts[0], json_keys)
        unified_chunk = utils.unify_counts(counts, base_json_keys, inner_keys=inner_keys)
        all_counts = utils.unify_dicts_recursive(all_counts, unified_chunk)
    
    # Consolidating total tweets by date
    consolidated = utils.consolidate_dict(all_counts)
    # Getting the 10 dates with more tweets
    max_value = utils.get_nth_value_from_dict(consolidated, 10, False)

    for max_date in max_value:
        max_user = utils.get_nth_value_from_dict(all_counts[max_date[0]],nth=1,asc=False)
        max_datetime = datetime.strptime(max_date[0], "%Y-%m-%d").date()
        max_user = max_user[0][0]
        result.append(tuple([max_datetime, max_user]))

    return result

def run_q1_time(file_path: str) -> None:
    start_time = time.time()
    
    mem_usage = memory_usage((q1_time, (file_path,)))
    
    end_time = time.time()
    
    print(f"Memory usage (MB): {max(mem_usage) - min(mem_usage)}")
    print(f"Execution time (seconds): {end_time - start_time}")

if __name__ == "__main__":
    test_file = os.path.join(current_path, "tweets_sample.json")
    #result = q1_time(None)
    result = q1_time(test_file)
    print(result)
    