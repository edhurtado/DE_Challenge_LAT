from typing import List, Tuple
import time
from datetime import datetime
import os
import pathlib
import re
import dask.dataframe as dd
import utils
from memory_profiler import memory_usage

current_path = pathlib.Path(__file__).parent.resolve()
FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
TEMP_FILE_NAME = os.path.join(current_path, "tweets.json.zip")

def extract_json_username(user_json):
    match = re.search(r"'username':\s*'([^']*)'", user_json)
    if match:
        return match.group(1)
    return None


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    global TEMP_FILE_NAME, FILE_URL
    required_cols = ["date", "user"]
    result = []
    remove_flag = False

    if not file_path:
        remove_flag = True
        # Download & Load JSON Zipped file from Google Drive
        utils.download_and_load_json_from_drive(FILE_URL, TEMP_FILE_NAME)
        os.remove(TEMP_FILE_NAME)
        file_path = "temp.json"

    df = dd.read_json(file_path, lines=True)[required_cols]

    df["username"] = df["user"].apply(extract_json_username, meta=("extracted_value", "object"))
    df = df.drop(columns=["user"]).copy()

    df["date"] = dd.to_datetime(df["date"]).dt.date
    top_dates = df["date"].value_counts().nlargest(10).compute()

    for date in top_dates.index:
        # Filter by date and get user with most tweets
        date_filtered = df[df["date"] == date]
        top_usernames = date_filtered["username"].value_counts().nlargest(1)
        top_usernames = top_usernames.compute()

        # Adding to the list
        if not top_usernames.empty:
            result.append((date, top_usernames.index[0]))
    if remove_flag:
        print(file_path, remove_flag)
        os.remove(file_path)

    return result

def run_q1_memory(file_path: str) -> None:
    start_time = time.time()
    
    mem_usage = memory_usage((q1_memory, (file_path,)))
    
    end_time = time.time()
    
    print(f"Memory usage (MB): {max(mem_usage) - min(mem_usage)}")
    print(f"Execution time (seconds): {end_time - start_time}")

if __name__ == "__main__":
    test_file = os.path.join(current_path, "tweets_sample.json")
    #test_file = "temp.json"
    #test_file = "test_file.json"
    result = q1_memory(None)
    #result = q1_memory(test_file)
    print(result)