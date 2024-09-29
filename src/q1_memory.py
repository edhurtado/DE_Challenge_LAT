from typing import List, Tuple
from datetime import datetime
import os
import pathlib
import re
import dask
import json
import dask.dataframe as dd
import pandas as pd
import utils

current_path = pathlib.Path(__file__).parent.resolve()
FILE_URL = "https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
TEMP_FILE_NAME = os.path.join(current_path, "tweets.json.zip")

# def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
#     df = dd.read_csv(file_path, usecols=['date', 'username'])
#     df['date'] = dd.to_datetime(df['date']).dt.date
#     top_dates = df['date'].value_counts().nlargest(10).compute()
#     result = [(date, df[df['date'] == date]['username'].value_counts().nlargest(1).index[0]) for date in top_dates.index]
#     return result


# def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
#     required_cols = ["date", "user"]
#     df = pd.read_json(file_path, lines=True)[required_cols]
#     user_normalized = pd.json_normalize(df['user'])
#     cleaned_df = df.drop(columns=['user']).join(user_normalized[['username']]).copy()

#     del df, user_normalized

#     cleaned_df['date'] = pd.to_datetime(cleaned_df['date']).dt.date
#     print(cleaned_df.head())
#     top_dates = cleaned_df['date'].value_counts().nlargest(10)
#     result = [(date, cleaned_df[cleaned_df['date'] == date]['username'].value_counts().nlargest(1).index[0]) for date in top_dates.index]
#     return result

def extract_json_username(user_json):
    match = re.search(r"'username':\s*'([^']*)'", user_json)
    if match:
        return match.group(1)
    return None


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    global TEMP_FILE_NAME, FILE_URL
    required_cols = ["date", "user"]
    result = []

    if not file_path:
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
    os.remove(file_path)

    return result

if __name__ == "__main__":
    test_file = os.path.join(current_path, "tweets_sample.json")
    #test_file = "temp.json"
    #result = q1_memory(None)
    result = q1_memory(test_file)
    print(result)