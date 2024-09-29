import utils
import pathlib
import os
import time
from memory_profiler import memory_usage
from typing import List, Tuple

current_path = pathlib.Path(__file__).parent.resolve()
FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
TEMP_FILE_NAME = os.path.join(current_path, "tweets.json.zip")

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    global FILE_URL, TEMP_FILE_NAME

    # Defining configuration parameters
    keys = ["content"]
    regex_col = "content"
    regex = utils.EMOJI_REGEX
    count_keys = None
    chunk_size = int(5e3)
    inner_keys = False

    if not file_path:
        # Download & Load JSON Zipped file from Google Drive
        json_data = utils.download_and_load_json_from_drive(FILE_URL, TEMP_FILE_NAME)
    else:
        # Load JSON if exists in path
        json_data = utils.load_json_lines(file_path)

    all_counts = dict()

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
    # Getting the 10 emojis most used
    max_value = utils.get_nth_value_from_dict(consolidated, 10, False)

    return max_value

def run_q2_time(file_path: str) -> None:
    start_time = time.time()
    
    mem_usage = memory_usage((q2_time, (file_path,)))
    
    end_time = time.time()
    
    print(f"Memory usage (MB): {max(mem_usage) - min(mem_usage)}")
    print(f"Execution time (seconds): {end_time - start_time}")

if __name__ == "__main__":
    test_file = os.path.join(current_path, "tweets_sample.json")
    #result = q2_time(None)
    result = q2_time(test_file)
    print(result)
    