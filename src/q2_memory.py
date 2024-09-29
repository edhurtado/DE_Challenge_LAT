from typing import List, Tuple
import os
import time
import pathlib
import utils
import dask.dataframe as dd
from memory_profiler import memory_usage

current_path = pathlib.Path(__file__).parent.resolve()
FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
TEMP_FILE_NAME = os.path.join(current_path, "tweets.json.zip")


def extract_emojis(text_series):
    # Define the regex for emoji extraction
    emoji_pattern = utils.EMOJI_REGEX

    # Extract emojis using the regex pattern
    emojis = text_series.str.findall(emoji_pattern)
    flattened_emojis = emojis.apply(lambda x: utils.flatten_list(x))

    return flattened_emojis

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    global TEMP_FILE_NAME, FILE_URL
    required_cols = ["content"]
    remove_flag = False

    if not file_path:
        remove_flag = True
        # Download & Load JSON Zipped file from Google Drive
        utils.download_and_load_json_from_drive(FILE_URL, TEMP_FILE_NAME)
        os.remove(TEMP_FILE_NAME)
        file_path = "temp.json"

    df = dd.read_json(file_path, lines=True)[required_cols]
    emoji_counts = (
        df[required_cols[0]]
        .map_partitions(extract_emojis)
        .explode()
        .value_counts()
    )

    result = emoji_counts.nlargest(10).compute().items()
    if remove_flag:
        os.remove(file_path)

    # Return result as requested
    return [(emoji, count) for emoji, count in result]

def run_q2_memory(file_path: str) -> None:
    start_time = time.time()
    
    mem_usage = memory_usage((q2_memory, (file_path,)))
    
    end_time = time.time()
    
    print(f"Memory usage (MB): {max(mem_usage) - min(mem_usage)}")
    print(f"Execution time (seconds): {end_time - start_time}")

if __name__=="__main__":
    test_file = os.path.join(current_path, "tweets_sample.json")
    test_file = "temp.json"
    #result = q2_memory(None)
    result = q2_memory(test_file)
    print(result)