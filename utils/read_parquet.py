import os
import pandas as pd
import pyarrow.parquet as pypq
import textwrap
from pathlib import Path
import time

if pd.__version__ >= '2.1':
    print('Using PyArrow strings!')
    pd.options.future.infer_string = True # Enable PyArrow strings for better performance and memory usage in large datasets

def peek_parquet(path):
    """
    Peeks at a parquet file (or a directory containing parquet files) and prints useful information.

    This function prints:
    - Path to the parquet file or directory.
    - The schema of the parquet file.
    - The number of pieces (fragments) in the parquet dataset.
    - The total number of rows across all fragments.
    - Displays 5 random rows from the first fragment as a sample.

    Args:
        path (str or Path): Path to the parquet file or directory containing parquet files.

    Example:
        peek_parquet('data/mydata.parquet')

    Notes:
        - The schema and random rows provide a quick preview of the dataset's structure.
        - Useful for understanding the structure and size of large parquet datasets without loading them fully.
    """
    
    # Convert the path to a Path object if it's a string.
    if isinstance(path, str):
        path = Path(path)
    
    # Load the parquet dataset, handling both individual files and directories.
    parq_file = pypq.ParquetDataset(path)

    # Count the number of fragments (pieces) in the parquet dataset.
    piece_count = len(parq_file.fragments)

    # Retrieve and format the schema as a string with indentation for readability.
    schema = textwrap.indent(parq_file.schema.to_string(), ' '*4)

    # Count the total number of rows across all fragments in the dataset.
    row_count = sum(frag.count_rows() for frag in parq_file.fragments)

    # Prepare a list of strings to print details about the dataset.
    st = [
        f'Name: {path.stem!r}',  # File or directory name without the extension.
        f'Path: {str(path)!r}',  # Full path to the file or directory.
        f'Files: {piece_count:,}',  # Number of pieces (fragments).
        f'Rows: {row_count:,}',  # Total number of rows.
        f'Schema:\n{schema}',  # Schema of the dataset.
        f'5 random rows:',  # Placeholder for the sample data.
    ]
    
    # Print the prepared details about the parquet file.
    print('\n'.join(st))
    
    # Display 5 rows from the first non-empty fragment as a sample.
    for fragment in parq_file.fragments:
        fragment_table = fragment.head(5)
        if len(fragment_table) > 0:
            # Convert the fragment to a pandas DataFrame and break
            sample_df = fragment_table.to_pandas()
            break
    
    try:
        display(sample_df)
    except NameError:
        print(sample_df)

    return

def read_parquet(
    path, engine='pyarrow', convert_dtypes=True, quiet=False, 
    year_range=None, filters = None, **args
):
    """
    Read a parquet file, with optional filtering by year range and dtype conversions.
    
    Args:
        path (str or Path): Path to the parquet file.
        engine (str): Parquet reading engine. Default is 'pyarrow'.
        convert_dtypes (bool): If True, automatically convert certain columns to 
                               more memory-efficient data types. Default is True.
        quiet (bool): If False, prints information about the reading process. Default is False.
        year_range (list or tuple, optional): A range of years [start_year, end_year] to filter rows. 
                                              Both inclusive. Default is None.
        filters (list): Filters to apply when reading the data. Example: `filters=[('score', '>', 90)]`. Defaults is None.
        **args: Additional arguments passed to `pd.read_parquet`. This can include:
            - columns (list): Specific columns to read. Example: `columns=['column1', 'column2']`
            - memory_map (bool): If True, memory-map the file for faster access. Example: `memory_map=True`
            - use_nullable_dtypes (bool): If True, use nullable data types. Example: `use_nullable_dtypes=True`
            - engine_kwargs (dict): Additional arguments for the engine. Example: `engine_kwargs={'param': value}`
    
    Returns:
        pd.DataFrame: DataFrame containing the data from the parquet file.

    Raises:
        FileNotFoundError: If the file cannot be found at the given path.
    
    Example:
        # Read a parquet file with filtering by year range
        df = read_parquet('data/mydata.parquet', year_range=[2010, 2020])
        
        # Read specific columns only
        df = read_parquet('data/mydata.parquet', columns=['work_id', 'title', 'num_authors', 'publication_date', 'publication_year', 'type'],)
        
        # Read with filters applied
        df = read_parquet('data/mydata.parquet', filters=[('num_authors', '>=', 1), ('num_authors', '<=', 30)])
        
        # Read with memory mapping
        df = read_parquet('data/mydata.parquet', memory_map=True)

    Returns:
        pd.DataFrame: DataFrame containing the data from the parquet file.

    Raises:
        FileNotFoundError: If the file cannot be found at the given path.
    
    Example:
        df = read_parquet('data/mydata.parquet', year_range=[2010, 2020])
    
    Notes:
        - The `year_range` parameter filters the rows based on publication year.
        - If `convert_dtypes` is True, the function attempts to optimize memory usage 
          by converting specific columns to more efficient data types (e.g., `category`, `float16`).
    """
    # Convert path to Path object if it's a string.
    if isinstance(path, str):
        path = Path(path)
    
    # Check if file has a .parquet extension, if not, try to add it.
    if not path.name.endswith('.parquet'):
        # Check if the file exists without the extension.
        dir_exists = path.exists()
        if not dir_exists:  # If it doesn't exist, add the parquet extension.
            if path.with_suffix('.parquet').exists():
                path = path.with_suffix('.parquet')
    
    # Get the base name of the file without the extension.
    name = path.stem

    # NOTE: ADD HERE STANDARD FILTERS YOU ALWAYS WANT TO HAVE IN EVERY READ REQUEST
    # Prepare filters based on the year range, if provided. 
    if filters is None:
        filters = []
    if year_range is not None:
        if not quiet:
            print(f'Filtering by years: {year_range[0]} -- {year_range[1]}')
        # Apply filters based on the name of the file.
        if name == 'works_referenced_works':
            filters.append([('work_publication_year', '>=', year_range[0]), ('work_publication_year', '<=', year_range[1])])
        else:
            filters.append([('publication_year', '>=', year_range[0]), ('publication_year', '<=', year_range[1])])
    if len(filters) == 0:
        filters = None
        
    # Print the read operation details unless quiet mode is enabled.
    if not quiet:
        print(f'\nReading {name!r} from {str(path)!r} using {engine=}')
    
    # Measure the time taken to read the parquet file.
    tic = time.time()
    df = pd.read_parquet(path, engine=engine, filters=filters, **args)
    toc = time.time()
    if not quiet:
        print(f'Read {len(df):,} rows from {path.stem!r} in {toc-tic:.2f} sec.')
  
    # Optionally convert certain columns to more memory-efficient dtypes.
    if convert_dtypes:
        tic = time.time()
        size_before = df.memory_usage(deep=True).sum() / 1024 / 1024 / 1024

        # Dictionary to store columns that need dtype conversion.
        string_cols_d = {}
        string_type = 'string[pyarrow]'  # PyArrow string type for optimized memory usage.

        # Loop through the DataFrame's dtypes and apply conversions.
        for col, dtype in df.dtypes.to_dict().items():
            # For pandas version <= 2.1, convert object or string to PyArrow strings.
            if pd.__version__ <= '2.1':
                if dtype == 'object' or dtype == 'string':
                    string_cols_d[col] = string_type
            # Convert specific columns to 'category' for memory efficiency.
            if col == 'type' or col == 'concept_name':
                if dtype != 'category':
                    string_cols_d[col] = 'category'
            # Convert 'publication_month' to 'uint8' to save memory.
            if col == 'publication_month':
                if dtype != 'uint8':
                    string_cols_d[col] = 'uint8'
            # Convert 'score' to 'float16' for better memory efficiency.
            if col == 'score':
                if dtype != 'float16':
                    string_cols_d[col] = 'float16'
            
        # Apply the dtype conversions.
        df = df.astype(string_cols_d) 

        # Measure the size after conversions and print memory usage stats.
        size_after = df.memory_usage(deep=True).sum() / 1024 / 1024 / 1024
        toc = time.time()
        if not quiet:
            print(f'Converting dtypes took {toc-tic:.2f} sec. Size before: {size_before:.2f}GB, after: {size_after:.2f}GB')

    # Display the first few rows if not in quiet mode.
    if not quiet:
        try:
            display(df.head(3))
        except NameError:
            print(df.head(3))
    
    # Return the DataFrame.
    return df