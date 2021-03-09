"""
Classic Map/Reduce-style dataset re-partitioning utility.

Given a set of input files in Parquet format and a numeric column name, shuffle all rows into the given no. of parts
(a set of numbered files or directories). The appropriate part ID for each row is set by simple 'value % num_parts' over
the given column value.

This is a mandatory requirement for using datasets in Funnel Rocket, since it ensures that all rows of each specific
user/group ID (or any other identifier you wish to partition by) are found in a single file only, thus no shuffle
is necessary at query time.

This utility is not part of Funnel Rocket package itself. Rather, it can assist in creating datasets of limited size -
since it only runs on a single machine. It does utilize multiple CPUs, so running on a beefy VM is an option for ad-hoc
larger jobs. For a robust & scalable solution, using PySpark or the like is advised.

TODO consider a distributed re-partitioning feature in Funnel Rocket (atm it's a read-only tool)
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import glob
import argparse
import sys
import pathlib
import math
import shutil
from multiprocessing import Pool, cpu_count

MAX_PART_NUMBER_DIGITS = 5


def get_part_path(outdir_path, i):
    """Helper for naming parts, with part number i padded by zeros (e.g. 'map/part-00037'), which makes filenames
    naturally sorted."""
    padded_part_number = str(i).zfill(MAX_PART_NUMBER_DIGITS)
    part_part = outdir_path / 'part-{}'.format(padded_part_number)
    return part_part


def get_worker_pool_size(num_files):
    """Returns how many worker processes to use."""
    available_cpus = max(cpu_count() - 1, 1)  # Leave one CPU alone, if there's more than one
    num_workers = min(available_cpus, num_files)  # No need for more workers than the actual no. of files to process
    return num_workers


def map_worker_task(task_id, input_file_path, outdir_path, partition_by, num_parts):
    """A single mapper task: read one input file and map its contents into N parts by the partition_by column."""
    table = pq.read_table(input_file_path, use_threads=False)  # Single-threaded, as we're using all CPUs anyway

    # id_mod_series will hold the appropriate part number for each row in the table
    id_series = table.column(partition_by).to_pandas()
    id_mod_series = id_series % num_parts

    # Iterate over part numbers. In each iteration, filter only the rows relevant for current part and save them
    for part_number in range(num_parts):
        # Create a boolean "mask" column (True only for rows where partition_by field maps to to current part no.)
        mask = (id_mod_series == part_number).values  # Get NumPy boolean array
        masked_table = table.filter(mask)

        # The file will be placed in the part folder, but it needs a unique name
        # (it could also be the value of i, but here we use the original file name)
        output_file_path = get_part_path(outdir_path, part_number) / 'from-{}.parquet'.format(input_file_path.stem)
        pq.write_table(masked_table, output_file_path)

        if part_number and part_number % 100 == 0:
            print(f"Task no. {task_id} for input file {input_file_path}: written {part_number} files so far... ")


def map_files(args):
    """Run map tasks for all input files."""
    files_list = glob.glob(args.input, recursive=True)
    if not files_list:
        sys.exit(f"No input files matching pattern '{args.input}'")

    map_dir_path = pathlib.Path(args.mapdir)
    existing_files = list(map_dir_path.glob('*'))
    if len(existing_files) > 0:
        if args.force:
            print(f"Recreating map output dir '{map_dir_path}'")
            shutil.rmtree(map_dir_path)
        else:
            sys.exit(f"Map output dir '{map_dir_path}' is not empty (consider using the --force)")

    map_dir_path.mkdir(parents=True, exist_ok=True)

    # Create sub-directory for each part - these directories are needed by all tasks that will emit their part,
    # so create them centrally here (and not inside a task)
    for i in range(args.parts):
        part_path = get_part_path(map_dir_path, i)
        if not part_path.exists():
            part_path.mkdir()

    pool_size = get_worker_pool_size(len(files_list))
    print(f"Input files found: {len(files_list)}, map output dir: {map_dir_path}, target partitions: {args.parts}, "
          f"CPUs: {cpu_count()}, pool size: {pool_size}")

    # Prepare a task for each input file
    pool = Pool(pool_size)
    tasks = []
    for task_id, input_file in enumerate(files_list):
        task_args = (task_id,
                     pathlib.Path(input_file),
                     map_dir_path,
                     args.partition_by,
                     args.parts)
        tasks.append(pool.apply_async(map_worker_task, task_args))

    # Actually run all tasks to completion
    [task.get() for task in tasks]

    # Ensure that the expected number of output files are found
    expected_outfiles_count = len(files_list) * args.parts
    outfiles_found = map_dir_path.glob('part-*/*.parquet')
    actual_outfiles_count = len(list(outfiles_found))

    if actual_outfiles_count != expected_outfiles_count:
        sys.exit(f"Expected {expected_outfiles_count} map output files, but found {actual_outfiles_count}")

    print(f"Map stage done, total of {actual_outfiles_count} files created")


def reduce_worker_task(part_map_path, part_reduce_path, rows_per_file, sort_by):
    """
    A single reduce task: load all intermediate map output files for its assigned part number,
    merging all rows into one partition.

    TODO to limit memory usage in real-world jobs, read files iteratively rather than all at once
    """
    dataset = ds.dataset(part_map_path, format="parquet")
    table = dataset.to_table()

    if sort_by:
        # Go through Pandas for sorting (this is heavy and very memory-hungry)
        df = table.to_pandas()
        df.sort_values(by=sort_by, inplace=True, ignore_index=True)
        # noinspection PyArgumentList
        table = pa.Table.from_pandas(df, schema=table.schema)

    if rows_per_file:
        num_files = math.ceil(table.num_rows / rows_per_file)
        for slice_number in range(num_files):
            if slice_number == (num_files - 1):  # Last slice (or only slice)
                length = None  # No lenght limit (see below)
            else:
                length = rows_per_file

            offset = slice_number * rows_per_file
            curr_slice = table.slice(offset, length)
            output_file_path = part_reduce_path / 'slice-{}.parquet'.format(slice_number)
            pq.write_table(curr_slice, output_file_path)
    else:
        output_file_path = str(part_reduce_path) + '.parquet'
        pq.write_table(table, output_file_path)


def reduce_files(args):
    """Run a reduce task for each part number in the target no. of partitions."""
    map_dir_path = pathlib.Path(args.mapdir)
    if not map_dir_path.is_dir():
        sys.exit(f"Map output dir '{map_dir_path}' does not exists, did you run the map stage?")

    num_part_dirs = len(list(map_dir_path.glob('part-*/')))
    if num_part_dirs != args.parts:
        sys.exit(f"Expected {args.parts} sub-dirs under map dir, but found {num_part_dirs}")

    reduce_dir_path = pathlib.Path(args.reducedir)
    existing_files = list(reduce_dir_path.glob('*'))
    if len(existing_files) > 0:
        if args.force:
            print(f"Recreating reduce output dir '{reduce_dir_path}'")
            shutil.rmtree(reduce_dir_path)
        else:
            sys.exit(f"Reduce output dir '{reduce_dir_path}' is not empty (consider using the --force)")

    reduce_dir_path.mkdir(parents=True, exist_ok=True)

    # Create sub-directory per output part - if allowing multi-file partitions
    if args.maxrows:
        for i in range(args.parts):
            part_path = get_part_path(reduce_dir_path, i)
            if not part_path.exists():
                part_path.mkdir()

    # Collect the input paths: we expect an existing sub-dir generated in the map stage, per each part number
    paths = []
    for i in range(args.parts):
        part_path = get_part_path(map_dir_path, i)
        if not part_path.exists():
            sys.exit(f"Part path '{part_path}' does not exist!")
        paths.append(part_path)

    # Prepate a task per part
    pool = Pool(get_worker_pool_size(len(paths)))
    tasks = []
    for task_number, part_path in enumerate(paths):
        part_reduce_path = get_part_path(reduce_dir_path, task_number)
        task_args = (part_path, part_reduce_path, args.maxrows, args.sort)
        tasks.append(pool.apply_async(reduce_worker_task, task_args))

    # Run all tasks to completion
    [task.get() for task in tasks]
    print(f"Reduce stage done, output parts are in directory: {reduce_dir_path}")


if __name__ == '__main__':  # Don't run in worker processes
    DEFAULT_NUM_PARTS = 16
    DEFAULT_INPUT_FILES = './data/**/*.parquet'
    DEFAULT_MAP_DIR = './map'
    DEFAULT_REDUCE_DIR = './reduce'

    parser = argparse.ArgumentParser(
        description='Partition Parquet files into N parts in two stages: map & reduce',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('partition_by', type=str,
                        help='Numeric column to partition by (e.g. userId, experimentId, ...)')
    parser.add_argument('--action', type=str,
                        choices=['all', 'map', 'reduce'], default='all',
                        help='Whether to run just the map or reduce stages')
    parser.add_argument('--input', type=str,
                        default=DEFAULT_INPUT_FILES,
                        help='A glob-style pattern of input files to map')
    parser.add_argument('--mapdir', type=str,
                        default=DEFAULT_MAP_DIR,
                        help='Root directory for map output')
    parser.add_argument('--reducedir', type=str,
                        default=DEFAULT_REDUCE_DIR,
                        help='Root directory for reduced files')
    parser.add_argument('--parts', type=int,
                        default=DEFAULT_NUM_PARTS,
                        help='number of partitions to emit')
    parser.add_argument('--force', default=False,
                        action='store_true',
                        help='Whether to overwrite existing files')
    parser.add_argument('--maxrows', type=int,
                        help='Optionally split the output files by a maximum N rows per file, to limit file size. '
                             'In this mode each part is a directory with one or more files, rather than a single file.')
    parser.add_argument('--sort', type=str, nargs='+',
                        help='Optional space-separated list of column names to sort by in the reduce stage. '
                        'By default, output files are not sorted. NOTE: This is memory- and compute-intensive.')

    args = parser.parse_args()
    if args.action in ['map', 'all']:
        map_files(args)
    if args.action in ['reduce', 'all']:
        reduce_files(args)
