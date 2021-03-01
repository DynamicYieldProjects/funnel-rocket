import argparse
import sys
from os import walk
import pandas as pd
from pandas import DataFrame

EVENTS_FILE = 'events.csv'
PROPS_FILE_1 = 'item_properties_part1.csv'
PROPS_FILE_2 = 'item_properties_part2.csv'
DS_FILES = {EVENTS_FILE, PROPS_FILE_1, PROPS_FILE_2}
COMMON_COLUMNS = {'categoryid', 'available', '790', '888'}


def create_feed(path: str) -> DataFrame:
    df = pd.read_csv(path)
    df = df[df['property'].isin(COMMON_COLUMNS)]
    first_value_per_item = df.groupby(["itemid", "property"])["value"].first()
    df = first_value_per_item.to_frame()
    df = df.unstack(level=-1)
    df.columns = df.columns.droplevel(0)
    return df


# noinspection PyPep8,PyBroadException
def ingest(args):
    input_path = args.input_path
    files_list = None
    try:
        (_, _, files_list) = next(walk(input_path))
    except Exception:
        pass
    if not files_list:
        sys.exit('No input files matching pattern {}'.format(input_path))
    if len(DS_FILES.intersection(set(files_list))) < 3:
        sys.exit('Missing one of these files: {}'.format(DS_FILES))
    df1 = create_feed('/'.join([input_path, PROPS_FILE_1]))
    df2 = create_feed('/'.join([input_path, PROPS_FILE_2]))
    combined_feed = df1.combine_first(df2)
    events = pd.read_csv('/'.join([input_path, EVENTS_FILE]))
    merged = pd.merge(events, combined_feed, how='inner', on='itemid')
    merged['790'] = merged['790'].str.split('n').str[1].astype(float).astype(int)
    merged.rename(columns={"790": "price"}, inplace=True)
    merged['available'] = merged['available'].fillna(0).astype(int).astype(bool)
    merged['categoryid'] = merged['categoryid'].astype('category')
    merged['event'] = merged['event'].astype('category')
    print(merged.info())
    print('writing to', args.output_path)
    if merged.shape != (2500516, 9):
        print('Warning: expected shape to be (2500516, 9) but got {}'.format(merged.shape))
    merged.to_parquet('/'.join([args.output_path, 'retailrocket.parquet']), engine='pyarrow')
    print('done')


if __name__ == '__main__':
    DEFAULT_INPUT_PATH = './scratch'
    DEFAULT_OUTPUT_PATH = './scratch'

    parser = argparse.ArgumentParser(description='Ingest https://www.kaggle.com/retailrocket/ecommerce-dataset/')
    parser.add_argument('--input_path', type=str, default=DEFAULT_INPUT_PATH,
                        help='The location of the dataset')
    parser.add_argument('--output_path', type=str, default=DEFAULT_OUTPUT_PATH,
                        help='Output location (should exist)')

    args = parser.parse_args()

    ingest(args)
