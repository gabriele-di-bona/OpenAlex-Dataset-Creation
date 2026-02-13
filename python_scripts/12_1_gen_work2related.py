import os
os.chdir("../") # resets notebook directory to repository root folder (DO ONLY ONCE!)
import polars as pl
import pandas as pd
import pyarrow.parquet as pypq
import textwrap
from pathlib import Path
import time

from tqdm import tqdm

import sys
# Add utils directory in the list of directories to look for packages to import
sys.path.insert(0, os.path.join(os.getcwd(),'utils'))
from read_parquet import *

works_by_topic_parquet_folder = "data/works_by_topic_parquet/"
works2related_by_topic_parquet_folder = "data/works2related_by_topic_parquet/"
os.makedirs(works2related_by_topic_parquet_folder, exist_ok=True)

topics = [topic[:-8] for topic in os.listdir(works_by_topic_parquet_folder)]

import argparse
parser = argparse.ArgumentParser(description='Run this in parallel to speed up.')

parser.add_argument("-ID", "--ID", type=int,
    help="The index of the topic in topics list (will be subtracted 1, start by 1) [default 1]",
    default=1)

arguments = parser.parse_args()
ID = arguments.ID - 1 # unfortunately I have to start this argument from 1 in shell. The -1 is added to start from 0
topic = topics[ID]

print("reading", flush=True)

all_works2primary_topic_df = read_parquet("data/all_works2primary_topic_parquet/all_works2primary_topic.parquet")

print("read", flush=True)

all_works2primary_topic_df.rename(
    columns={"id": "related_work_id", "date": "related_publication_date", "primary_topic": "related_primary_topic"},
    inplace = True
)
print("renamed", flush=True)

def generate_related_expansion(origin_works_parquet_file_path, all_works2primary_topic_df):
    works_df = read_parquet(origin_works_parquet_file_path, columns = ["id", "date", "related_works", "primary_topic"], quiet = True)
    
    # Step 1: Explode related works into rows
    works_df = works_df[works_df["related_works"].apply(lambda x : len(x) > 0)]
    works_df["related_works"] = works_df["related_works"].apply(lambda x : x.split(";"))
    works_df = works_df.explode("related_works").rename(columns={"id": "work_id", "date": "publication_date", "related_works": "related_work_id"})

    # Step 2: Join with metadata of the related works
    works_df = works_df.merge(all_works2primary_topic_df, on="related_work_id", how="inner")

    # Step 3: Select and reorder the final columns
    works_df = works_df[[
        "work_id", "publication_date", "primary_topic",
        "related_work_id", "related_publication_date", "related_primary_topic"
    ]]

    return works_df
    
def create_works2related_df(origin_works_parquet_file_path, destination_topic_parquet_file_path, all_works2primary_topic_df, compression='brotli', do_peek = True, do_print = True):
    works2related_df = generate_related_expansion(origin_works_parquet_file_path, all_works2primary_topic_df)
    works2related_df.to_parquet(destination_topic_parquet_file_path, compression=compression)
    if do_print:
        print(f"Successfully generated {destination_topic_parquet_file_path} using {origin_works_parquet_file_path}.", flush=True)
    if do_peek:
        if do_print:
            print("Here's a peek.", flush=True)
        peek_parquet(destination_topic_parquet_file_path)

# MAIN CODE

origin_works_parquet_file_path = works_by_topic_parquet_folder+topic+".parquet"
destination_topic_parquet_file_path = works2related_by_topic_parquet_folder+topic+".parquet"
create_works2related_df(origin_works_parquet_file_path, destination_topic_parquet_file_path, all_works2primary_topic_df, do_peek = True, do_print = True)
