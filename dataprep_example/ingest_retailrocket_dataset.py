import sys
import time
import argparse
from pathlib import Path
from contextlib import contextmanager
import pandas as pd
from pandas import DataFrame

EVENTS_FILE = 'events.csv'
PROPS_FILE_1 = 'item_properties_part1.csv'
PROPS_FILE_2 = 'item_properties_part2.csv'
INPUT_FILENAMES = {EVENTS_FILE, PROPS_FILE_1, PROPS_FILE_2}
ITEM_PROPERTY_COLUMNS = {'categoryid', 'available', '790', '888'}
EXPECTED_EVENT_COUNT = 2_500_516


def progress_msg(msg: str):
    print(f"\033[33m{msg}\033[0m")  # Yellow, just yellow


@contextmanager
def timed(caption: str):
    start = time.time()
    yield
    total = time.time() - start
    print(f"Time to {caption}: {total:.3f} seconds")


# Read item properties files, filter for relevant columns and 'pivot' its structure from rows to columns
def read_item_props(filepath: Path) -> DataFrame:
    df = pd.read_csv(filepath)
    df = df[df['property'].isin(ITEM_PROPERTY_COLUMNS)]
    first_value_per_item = df.groupby(["itemid", "property"])["value"].first()
    df = first_value_per_item.to_frame()
    df = df.unstack(level=-1)
    df.columns = df.columns.droplevel(0)
    return df


def ingest(path: Path):
    with timed("read & transform item properties of all products"):
        item_props_tempfile = path / "item_props.parquet"
        if item_props_tempfile.exists():
            progress_msg(f"Reading item properties from cached file {item_props_tempfile}")
            item_props_df = pd.read_parquet(item_props_tempfile)
        else:
            progress_msg("Reading item properties... (this takes a bit)")
            item_props_df1 = read_item_props(path / PROPS_FILE_1)
            item_props_df2 = read_item_props(path / PROPS_FILE_2)
            item_props_df = item_props_df1.combine_first(item_props_df2)
            progress_msg(f"Storing item properties to {item_props_tempfile} for faster re-runs...")
            item_props_df.to_parquet(item_props_tempfile)

    with timed("read & transform user events"):
        progress_msg("Reading user events...")
        events = pd.read_csv(path / EVENTS_FILE)
        progress_msg("Joining events with item properties...")
        events = pd.merge(events, item_props_df, how='inner', on='itemid')

        progress_msg("Making columns more queryable...")
        events['price'] = events['790'].str[1:].astype(float) / 1000
        events.drop(columns=['790'], inplace=True)
        events['available'] = events['available'].astype(int).astype(bool)
        events['categoryid'] = events['categoryid'].astype('category')
        events['event'] = events['event'].astype('category')
        events.rename(columns={'888': 'cryptic_attrs'}, inplace=True)
        progress_msg("Storing 'cryptic_attrs' also as categorical column 'cryptic_attrs_cat'...")
        events['cryptic_attrs_cat'] = events['cryptic_attrs'].astype('category')
        events.reset_index(drop=True)

    progress_msg("Excerpt from final DataFrame:")
    print(events)
    progress_msg("Columns types (a.k.a. dtypes):")
    print(events.dtypes)
    progress_msg("Breakdown of event types:")
    print(events['event'].value_counts())

    if len(events) != EXPECTED_EVENT_COUNT:
        progress_msg(f"WARNING: Expected {EXPECTED_EVENT_COUNT} events, but final DataFrame has {len(events)}")

    output_file = path / 'retailrocket.parquet'
    events.to_parquet(output_file)
    col_memory_sizes = (events.memory_usage(deep=True) / 1024 ** 2).round(decimals=2)
    progress_msg(f'Size of DataFrame columns in memory (in MB):')
    print(col_memory_sizes)
    progress_msg(f"==> Saved output file to: {output_file}, size: {output_file.stat().st_size / 1024 ** 2:.1f}MB")

    with timed("load file - all columns"):
        pd.read_parquet(output_file)

    with timed("load file - just the 'cryptic_attrs' column"):
        pd.read_parquet(output_file, columns=['cryptic_attrs'])

    with timed("load file - just the 'cryptic_attrs_cat' column"):
        pd.read_parquet(output_file, columns=['cryptic_attrs_cat'])

    with timed("load file - all columns *except* these two"):
        cols = [col for col in events.dtypes.index
                if col not in ['cryptic_attrs', 'cryptic_attrs_cat']]
        pd.read_parquet(output_file, columns=cols)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest RetailRocket dataset (to download: https://www.kaggle.com/retailrocket/ecommerce-dataset/)')
    parser.add_argument(
        'path', type=str,
        help='Directory where downloaded dataset files are found and output file will be written')
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists() or not path.is_dir():
        sys.exit(f'No such directory: {path}')
    files_in_path = {f.name for f in path.iterdir()}
    if not files_in_path >= INPUT_FILENAMES:
        sys.exit(f'Missing one or more input files: {INPUT_FILENAMES}')
    ingest(path)
