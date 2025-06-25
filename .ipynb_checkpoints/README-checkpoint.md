# OpenAlex Dataset Creation  
*Last updated: June 2025*  
*By **Gabriele Di Bona***

---

## Repository Structure

All data and notebooks are organized as follows:

- **Repository root:** `~/`
- **Data folder:** `~/data/`
- **Original snapshot:** `~/data/openalex-snapshot/`
- **Jupyter notebooks:** `~/notebooks/`
- **Python scripts:** `~/python_scripts/`
- **Bash scripts:** `~/bash_scripts/`
- **Utils:** `~/utils/`

### Download Tips

If you plan to download the snapshot in multiple versions or times:
- Duplicate this repository into a new folder named with the month and year (e.g., `openalex-june2025`)
- Or modify the current repository to update the snapshot incrementally

---

## Download the OpenAlex Snapshot

Download the OpenAlex snapshot into the folder `~/data/openalex-snapshot/`. There are two main options:

1. **Via AWS CLI (official method)**  
   Follow [OpenAlex’s documentation](https://docs.openalex.org/download-all-data/download-to-your-machine) and run:
   ```
   aws s3 sync "s3://openalex" "data/openalex-snapshot" --no-sign-request
   ```

2. **Via Python notebook (no `aws` required)**
   Run the notebook:

   ```
   ~/notebooks/1_download_snapshot.ipynb
   ```

   This script:

   * Scrapes [OpenAlex’s datadump browse page](https://openalex.s3.amazonaws.com/browse.html)
   * Downloads missing files to your local `/data/` folder
   * Can be easily converted to a `.py` script
   * Was used on the QMUL cluster and completed in under a day

### Snapshot Size (June 2025 Release)

The full snapshot is approximately **417 GB**. Below are the subfolder sizes:

| Size | Subfolder      |
| ---: | -------------- |
| 1.5K | `domains`      |
|  10K | `fields`       |
| 143K | `subfields`    |
| 3.5M | `publishers`   |
| 4.9M | `topics`       |
|  14M | `funders`      |
|  89M | `concepts`     |
| 133M | `merged_ids`   |
| 182M | `institutions` |
| 335M | `sources`      |
|  57G | `authors`      |
| 359G | `works`        |

---

## Create Tables from the Snapshot

This repository extracts and processes relevant tables using the **primary topic** assigned by OpenAlex.

* Each work is assigned up to three topics.
* The **primary topic** is the one with the highest score.
* Works without a topic are **excluded** from downstream processing.

### What is saved?

Each topic will have:

* A `*.parquet` file containing all works assigned that topic as primary
* Associated tables containing information about authors, affiliations, funders, references, and keywords

Currently:

* **Concepts** information is not retained (deprecated by OpenAlex)
* **Topics and keywords** are preserved for each work

---

## Processing Workflow

After downloading the snapshot:

1. Run the numbered notebooks in `~/notebooks/`:

   ```
   ~/notebooks/X_create_SOMETHING.ipynb
   ```

2. Intermediate data is first saved as `*.csv` files

3. Convert all intermediate `.csv` files to compressed `.parquet` files using:

   ```
   ~/notebooks/aux_convert_csv2parquet.ipynb
   ```

4. After conversion, original `.csv` files **can be deleted**

5. Generate further tables running the numbered notebooks Run the numbered notebooks `~/notebooks/X_gen_SOMETHING.ipynb`

## Notes

* Compressed `parquet` files use **Brotli** to reduce size and support fast reads and filtered loading.
* After notebooks `02` to `07` are completed, the original snapshot folder can also be deleted.
* Notebooks `02` to `07` can be run at the same time or one after another, they do not interfere with each other, while the following ones require the previous notebooks to be run and the `.csv` files to be transformed into `.parquet` files.
* The notebooks that require more times are those that go through every work folder in the snapshot (time between half and one day with one core, e.g. `~/notebooks/2_create_topic_works.ipynb`), and those generating reference and citation tables. Since the reference table process should take around 3 days on a single core, this has been parallelized with a script.
* Some files contain a lot of information, so the RAM can be used quite intensively. The most RAM consuming process is transforming the authors table from `.csv` to `.parquet`, reaching a maximum of about 80G of RAM used.
* If RAM or hard disk is a problem, the authors table can be split, either in multiple files keeping all columns, or removing some columns, for example removing `works_count_by_year` and `cited_by_count_by_year`. Similarly, for the text of each work, it could be reduced by either removing the abstract column or putting it into a different file. Nevertheless, if all these files can be created and stored with success, since they are `.parquet` files, they can be read with a smaller amount of memory by pre-selecting to load only certain columns and/or rows, significantly helping in the usage of these files
* For citation networks purposes, notice that every citation is saved twice, once as a reference in `~/data/works2references_by_topic_parquet/` and once as a citation in `~/data/works2citations_by_topic_parquet/`. In particular, if one wants to get all references and citations from and to a certain topic, one should be careful to remove duplicates, especially for those within the same topic, as the same (but reversed) should appear in both the topic reference table and citation table.
* This modular pipeline enables both step-by-step exploration and scalable transformation for large-scale OpenAlex analysis.

---

## First processing breakdown

### Create works table by topic

The first step is to go over all works in the OpenAlex snapshot and extract the relevant information. After parsing the primary topic of each work, a row is appended to the corresponding file in `~/data/works_by_topic_csv/`, based on that primary topic.

This processing is handled in `~/notebooks/2_create_topic_works.ipynb`. 

> ⚠️ **Important**: This notebook appends data. If the folder `~/data/works_by_topic_csv/` is not empty, execution will stop to avoid appending duplicate data. Make sure the folder is empty before running.

Once completed, the related section in `~/notebooks/aux_convert_csv2parquet.ipynb` can be executed to convert the generated `*.csv` files into compressed and sorted `*.parquet` files (sorted by date and work id), saved under `~/data/works_by_topic_parquet/`. After this conversion, the intermediate `*.csv` files in `~/data/works_by_topic_csv/` can be safely deleted.



#### Table structure

Each topic-specific table includes the following columns:

```
['id', 'date', 'type', 'language', 'journal', 'doi', 'authors', 'topics', 'references', 
'sdg', 'keywords', 'grants', 'primary_topic']
```

- **`id`**: A unique OpenAlex identifier (e.g., `W123456`) for the work.
- **`date`**: The publication date of the work.
- **`type`**: The type of publication (e.g., `article`, `preprint`, `review`, etc.).
- **`language`**: ISO 2-letter language code (e.g., `en`).
- **`journal`**: The OpenAlex ID of the journal (if available), or an empty string.
- **`doi`**: The work’s DOI without the `https://doi.org/` prefix.
- **`authors`**: A compact string representation of all authors in the format  
  `author1_institutionA|institutionB_F;author2_institutionC_T;...`,  
  where `T`/`F` indicates corresponding author status.
- **`topics`**: A semicolon-separated list of topics with classification scores, formatted as  
  `topic1_score1;topic2_score2;...` with scores between 0 and 1.
- **`references`**: A semicolon-separated list of OpenAlex work IDs cited by the work.
- **`sdg`**: Sustainable Development Goals, formatted as  
  `sdg1_score1;sdg2_score2;...`.
- **`keywords`**: Keywords with scores, formatted as  
  `keyword1_score1;keyword2_score2;...`.
- **`grants`**: Funding information formatted as  
  `awardid_funderid;...`.
- **`primary_topic`**: The main topic ID associated with the work.

#### CSV generation logic

For each JSON line in the gzipped OpenAlex dump, a `row` is built if the work contains all required fields (`primary_topic`, `publication_date`, `authorships`). This row is appended to a per-topic buffer. Once the buffer exceeds a threshold (`BUFFER_SIZE = 1000`), it is flushed to a CSV file in append mode.

### Create author table

This notebook (`~/notebooks/3_create_authors.ipynb`) processes the OpenAlex author dump and builds a comprehensive author table, saved by chunks into `~/data/authors/` in CSV format.

> ⚠️ **Important**: This notebook appends data. If the folder `~/data/authors/` is not empty, execution will stop to avoid duplication. Make sure to empty the folder before running.

After execution, run the related section in `~/notebooks/aux_convert_csv2parquet.ipynb` to convert the CSVs into a single `authors.parquet` file with `brotli` compression. The intermediate CSVs can then be deleted.

#### Table structure

Each row has the following columns:

```
['id', 'display_name', 'orcid', 'works_count', 'h_index', 'cited_by_count', 
 'topics', 'affiliations', 'works_count_by_year', 'cited_by_count_by_year']
```

- `id`: OpenAlex author ID (e.g., `A123456`)
- `display_name`: Full name of the author
- `orcid`: ORCID (if present), without the prefix
- `works_count`: Total number of works authored
- `h_index`: Author's h-index
- `cited_by_count`: Total citations received
- `topics`: Topic IDs and associated work counts, formatted as `T1234_5;T5678_3`
- `affiliations`: Institution IDs, country codes, and years, formatted as `I123_US_2018_2019;I234_UK_2020`
- `works_count_by_year`: Yearly work counts, formatted as `3_2018;5_2019`
- `cited_by_count_by_year`: Yearly citation counts, formatted as `10_2018;12_2019`

#### Extraction logic

Each author record is parsed and saved if valid:
- ORCID is extracted if present
- Topics and affiliations are formatted as semicolon-separated strings
- If structured affiliations are missing, the last known institution is used
- Yearly counts are included only if greater than zero
- Rows are buffered and flushed every 1000 authors to CSV

### Create work-to-text table by topic

This notebook (`~/notebooks/4_create_works2text.ipynb`) extracts the `title` and `abstract` fields from the OpenAlex works, grouped by their primary topic. The output is saved into topic-specific CSV files under `~/data/works2text_by_topic_csv/`.

> ⚠️ **Important**: This notebook appends data. If the folder `~/data/works2text_by_topic_csv/` is not empty, execution will stop to avoid appending duplicate data. Make sure the folder is empty before running.

After extraction, run the related section in `~/notebooks/aux_convert_csv2parquet.ipynb` to convert the CSVs into `*.parquet` files saved under `~/data/works2text_by_topic_parquet/`.

#### Table structure

Each topic-specific file contains the following columns:

```
['id', 'date', 'title', 'abstract']
```

- `id`: OpenAlex work ID (e.g., `W123456`)
- `date`: Publication date of the work
- `title`: Title of the work, with `;` characters replaced by `.`
- `abstract`: Abstract reconstructed from the OpenAlex `abstract_inverted_index`, also replacing `;` with `.`

#### Extraction logic

For each valid work (i.e., having `primary_topic`, `publication_date`, and `authorships`):

- The primary topic is extracted using the highest-ranked OpenAlex classification.
- The title and abstract are preprocessed to remove semicolons.
- Records are buffered in memory grouped by topic.
- Buffers are flushed to per-topic CSV files every 1000 rows.
- After all files are processed, the remaining buffers are flushed.

This results in one CSV per topic containing all textual metadata for the relevant works.

### Create institution table

This notebook (`~/notebooks/5_create_institutions.ipynb`) extracts metadata about institutions from the OpenAlex snapshot. The data is stored in a single flat table saved as one or more CSV files in the folder `~/data/institutions/`.

> ⚠️ **Important**: This notebook appends data. If the folder `~/data/institutions/` is not empty, execution will stop to avoid appending duplicate data. Ensure the folder is empty before running this script.

After extraction, the corresponding section of `~/notebooks/aux_convert_csv2parquet.ipynb` can be used to convert the CSV files into a single `*.parquet` file stored in `~/data/institutions.parquet`.

#### Table structure

The institution table includes the following columns:

```
['id', 'display_name', 'type', 'country_code', 'country', 'city', 'coord', 'works_count']
```

- `id`: OpenAlex ID of the institution (e.g., `I123456`).
- `display_name`: Name of the institution, with semicolons (`;`) replaced by periods (`.`).
- `type`: Type of the institution (e.g., `education`, `nonprofit`, etc.).
- `country_code`: ISO 2-letter country code (e.g., `US`, `GB`).
- `country`: Full country name (e.g., `United States`).
- `city`: City of the institution.
- `coord`: A string combining latitude and longitude as `latitude_longitude`. If unavailable, left empty.
- `works_count`: Number of works affiliated with this institution.

The script processes all lines from the `institutions` dump and appends valid rows into a buffer. Buffers are written to disk every 1000 rows to avoid memory overload, and flushed completely at the end of the script.

### Create funder table

This notebook (`~/notebooks/6_create_funders.ipynb`) extracts metadata about funding organizations from the OpenAlex snapshot and stores them in a flat table saved as CSV files in the folder `~/data/funders/`.

> ⚠️ **Important**: If the destination folder `~/data/funders/` is not empty, execution will halt to avoid appending duplicate rows. Ensure the folder is clean before running the script.

After running this notebook, use the relevant section in `~/notebooks/aux_convert_csv2parquet.ipynb` to convert the output CSV files into a single `funders.parquet` file in the same folder. The CSV files can then be safely deleted.

#### Table structure

Each row in the funders table corresponds to a funder entry and includes the following fields:

```
['id', 'display_name', 'country_code', 'grants_count', 'works_count']
```

- `id`: OpenAlex funder ID (e.g., `F123456`).
- `display_name`: Name of the funder (with semicolons replaced by periods).
- `country_code`: ISO 2-letter code of the country (e.g., `US`, `GB`).
- `grants_count`: Number of grants recorded for this funder.
- `works_count`: Number of works that acknowledge this funder.

Rows are buffered and flushed to disk every 1000 entries to optimize performance. The buffer is fully flushed again at the end of the script.

### Create topics table

This notebook (`~/notebooks/7_create_topics.ipynb`) extracts metadata about the topics assigned to works by OpenAlex and stores them as CSV files under `~/data/topics/`.

> ⚠️ **Important**: If the destination folder `~/data/topics/` already contains files, the script will stop to avoid appending duplicate rows. Make sure to empty the folder before running the notebook.

After this extraction, run the relevant section of `~/notebooks/aux_convert_csv2parquet.ipynb` to convert the resulting CSV files into a single compressed `topics.parquet` file. Once the conversion is done, the intermediate CSVs can be deleted.

#### Table structure

Each row in the topics table represents a topic entry with hierarchical and statistical metadata:

```
['topic_id','display_name','description','keywords','wikipedia',
 'subfield_id','subfield_name','field_id','field_name',
 'domain_id','domain_name','sibling_ids', 'works_count', 'cited_by_count']
```

- `topic_id`: OpenAlex topic ID (e.g., `T123456`).
- `display_name`: Topic name (e.g., "Network Science").
- `description`: Brief description of the topic.
- `keywords`: Underscore-joined string of keywords.
- `wikipedia`: URL to the associated Wikipedia page, if available.
- `subfield_id` / `subfield_name`: ID and name of the subfield the topic belongs to.
- `field_id` / `field_name`: ID and name of the broader field.
- `domain_id` / `domain_name`: ID and name of the overarching domain.
- `sibling_ids`: Underscore-joined list of sibling topic IDs.
- `works_count`: Number of works assigned to this topic.
- `cited_by_count`: Total citations received by works under this topic.

Buffered rows are flushed in chunks of 1000 and finally at the end of the process.

### Convert CSVs to Parquet format

After generating all intermediate `*.csv` files, the notebook `~/notebooks/aux_convert_csv2parquet.ipynb` handles their conversion into optimized `*.parquet` files. These files are saved into appropriate subfolders under `~/data/` and can be used in all following steps.

Each conversion follows these principles:
- The CSV is read using **Polars** for speed and lower memory usage.
- All column types are inferred from the data, usually **strings** or **integers**.
- Each resulting `.parquet` file is **sorted by `date` and then `id`** or **works_count**, if those columns are available and relevant.
- The files are saved with **Brotli compression**, significantly reducing their size without impacting access speed.

The original `*.csv` files are saved in:
- `~/data/works_by_topic_csv/`
- `~/data/works2text_by_topic_csv/`
- `~/data/authors/`
- `~/data/institutions/`
- `~/data/funders/`
- `~/data/topics/`

After the conversion, the resulting `.parquet` files are saved in:
- `~/data/works_by_topic_parquet/`
- `~/data/works2text_by_topic_parquet/`
- `~/data/authors/`
- `~/data/institutions/`
- `~/data/funders/`
- `~/data/topics/`

After a successful conversion, you can safely delete the corresponding CSV files to free up space.

The conversion logic used in the notebook resembles the following:

```python
def convert_csv2parquet(csv_file_path, parquet_file_path, sort_by=["date", "id"], separator=';', compression='brotli'):
    import polars as pl
    import os

    df = pl.read_csv(csv_file_path, separator=separator, infer_schema_length=1000)
    df = df.sort(by=sort_by)  # Ensure consistent order if applicable
    os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)
    df.write_parquet(parquet_file_path, compression=compression)
    print(f"Successfully converted {csv_file_path} to {parquet_file_path}")
```

This approach is used consistently across all file types. For example:
- Works (by topic) files are sorted and saved by topic ID into `~/data/works_by_topic_parquet/`.
- Author and institution tables are similarly converted into a unique file, but sorted with other columns since date here is not relevant.

The conversion notebook is modular, so you can re-run specific cells for converting individual folders if needed.

#### Summary of conversions

| CSV Folder                          | Parquet Folder                         | Sorted by                                         | Separator |
|-------------------------------------|----------------------------------------|---------------------------------------------------|-----------|
| `data/works_by_topic_csv/`          | `data/works_by_topic_parquet/`         | `["date", "id"]`                                  | `,`       |
| `data/works2text_by_topic_csv/`     | `data/works2text_by_topic_parquet/`    | `["date", "id"]`                                  | `;`       |
| `data/authors/`                     | `data/authors/`                        | `["h_index", "cited_by_count", "works_count"]`    | `,`       |
| `data/institutions/`                | `data/institutions/`                   | `["works_count"]`                                 | `;`       |
| `data/funders/`                     | `data/funders/`                        | `["grants_count", "works_count"]`                 | `;`       |
| `data/topics/`                      | `data/topics/`                         | `["works_count", "cited_by_count"]`               | `;`       |


---

## Second processing breakdown

### Generate work-topic and work-year tables

After extracting the complete topic-based works dataset, we generate two additional utility tables:

- `work2topic`: A simplified version of the works table, storing the top-3 topic classifications for each work (`~/notebooks/8_gen_work2topic.ipynb`).
- `work2year`: A simplified version of the works table, storing the year of publication and number of authors per work (`~/notebooks/9_gen_work2year.ipynb`).

These outputs are stored as individual `.parquet` files per topic in the folders:

- `data/works2topic_by_topic_parquet/`
- `data/works2year_by_topic_parquet/`

Both use **Brotli compression** and are read using **Polars**.

#### Work2topic table

This table is generated from `data/works_by_topic_parquet/` and keeps only the `"id"`, `"date"` and `"topics"` columns.  
Each topic string is split to extract the top 3 topics and their scores.

Each work gets expanded into six new fields: `t0, s0, t1, s1, t2, s2`  
representing the top 3 topics and their classification scores.

Example row:
```
id            | date       | t0     | s0    | t1     | s1    | t2     | s2
W4410793892   | 2025-05-28 | T14064 | 0.863 | T12028 | 0.769 | T14356 | 0.735
```

#### Work2year table

This table is generated from the same topic-based works, keeping only `"id"`, `"date"`, and `"authors"`.  
The date is truncated to the year (`"year"`), and the number of authors (`"n_authors"`) is counted from the `authors` field.

Each row has this structure:
```
id            | year | n_authors
W4410793892   | 2025 | 3
```

### Generate Author–Work Table

This notebook (`~/notebooks/10_gen_author2work.ipynb`) constructs a mapping of **authors to works** from the topic-split OpenAlex works dataset. Each row links an `author_id` to a `work_id`, together with metadata such as date, institution, and whether the author is the corresponding author.

The output is stored in:

```
data/author2work_by_topic_parquet/
```

Each file is a `.parquet` file per topic and is compressed using **Brotli**.

#### Output structure

The following columns are extracted:

- `author_id`: Cleaned author identifier
- `work_id`: Corresponding work ID
- `date`: Date of publication
- `primary_affiliation`: First listed institution for this author-work pair
- `is_corresponding`: Boolean flag (True if this author is marked as corresponding)

Example row:
```
author_id     | work_id       | date       | primary_affiliation | is_corresponding
A1234567890   | W4410793892   | 2025-05-28 | I998877             | True
```

#### Extraction logic

The `authors` column of each work is exploded. Each element in the form  
`author_id_affiliations_correspondingFlag` is parsed into:

- `author_id`: before first `_`
- `primary_affiliation`: first item of the institution block (split by `|`)
- `is_corresponding`: `T` if present as the third part; `F` otherwise

The logic supports two modes:

- **Fast**: Uses `Polars` to explode and transform the full column in memory
- **Slow (low-memory)**: Iterates row by row using `pandas`, better for limited RAM environments

### Generate All Works–Primary Topic Table

This notebook (`~/notebooks/11_gen_all_works2primary_topic.ipynb`) consolidates all works from topic-split `.parquet` files into a single table mapping each work to its `primary_topic`.

The output is saved in:

```
data/all_works2primary_topic_parquet/
```

This folder will contain a single compressed `.parquet` file.

#### Output structure

Each row in the output file represents one work, with the following fields:

- `id`: Work ID (e.g., `W4410793892`)
- `date`: Publication date (e.g., `2025-05-28`)
- `primary_topic`: Primary topic identifier (e.g., `T14064`)

Example row:
```
id             | date       | primary_topic
W4410793892    | 2025-05-28 | T14064
```

#### Extraction logic

1. Loop through all `.parquet` files in `data/works_by_topic_parquet/`.
2. From each file, load the columns:
   - `id`
   - `date`
   - `primary_topic`
3. Concatenate the rows into a single DataFrame.
4. Save to a single file in `data/all_works2primary_topic_parquet/all_works2primary_topic.parquet` using Brotli compression.


### Generate Work–Reference Table

This notebook (`~/notebooks/12_gen_work2references.ipynb`) creates a table of **work-to-reference** relationships, where each row maps a citing work (`work_id`) to a referenced work (`referenced_work_id`), along with both publication dates and primary topics.

The input data is sourced from topic-split OpenAlex works in:

```
data/works_by_topic_parquet/
```

The reference metadata is joined from:

```
data/all_works2primary_topic_parquet/all_works2primary_topic.parquet
```

The output files (one per topic) are saved in:

```
data/works2references_by_topic_parquet/
```

#### Output structure

Each `.parquet` file contains the following columns:

- `work_id`: ID of the citing paper
- `publication_date`: Date of the citing paper
- `primary_topic`: Topic of the citing paper
- `referenced_work_id`: ID of the cited paper
- `referenced_publication_date`: Date of the cited paper
- `referenced_primary_topic`: Topic of the cited paper

Example row:
```
work_id        | publication_date | primary_topic | referenced_work_id  | referenced_publication_date | referenced_primary_topic
W4410793892    | 2025-05-28       | T14064        | W1234567890         | 2020-03-10                  | T19332
```

#### Extraction logic

1. **Explode references**:  
   For each work, the semicolon-separated `references` field is split and exploded into multiple rows.

2. **Join reference metadata**:  
   Each `referenced_work_id` is matched with its corresponding publication date and topic from the full works file.

3. **Assemble final table**:  
   Columns are selected and renamed for clarity.

#### Parallel Execution on Cluster

Because this process is slow (3+ days on single-core), we use cluster jobs to parallelize by topic. The Python script to generate each topic's file is:

```
python_scripts/12_gen_work2references.py
````

This script takes a topic index (`-ID`) as argument for the topics list, e.g., for the 1489-th topic (in python starting from 0, this would correspond to index 1488):
```
python 12_gen_work2references.py -ID 1489
````

The SLURM/SGE job array is defined in:
```
bash_scripts/gen_work2references.sh
```

This uses a job range from `1` to `4516` with `1 core`, `40GB RAM`, and `1 hour` per task, as it required 34GB of RAM during execution


### Generate Work–Citations CSVs (by Cited Topic)
This notebook (`~/notebooks/13_gen_work2citations.ipynb`) processes the per-topic `works2references` tables to produce a **reversed citation view**, where each row lists a work and all the citing papers **grouped by the topic of the referenced work**.

Each CSV contains all papers citing works in a specific topic. The output is stored in:

```
data/works2citations_by_topic_csv/
```

Each file is named `{topic_id}.csv` and contains citations where the **referenced work belongs to that topic**.

As usual, these csv files can be transformed into parquet using `~/notebooks/aux_convert_csv2parquet.ipynb`.

#### Output structure

Similarly to the work-reference table, each row in the CSVs contains:

- `work_id`: ID of the citing paper
- `primary_topic`: Topic of the citing paper
- `publication_date`: Publication date of the citing paper
- `referenced_work_id`: ID of the referenced paper
- `referenced_primary_topic`: Topic of the referenced paper (== topic of this file)
- `referenced_publication_date`: Publication date of the referenced paper

#### Extraction logic

- Input files:  
  Topic-split `.parquet` files from `data/works2references_by_topic_parquet/`

- For each reference, the citing record is **reversed** and buffered under the referenced topic.

- Buffers are flushed to CSV when they reach `BUFFER_SIZE = 5000`.

- CSVs are written in append mode with UTF-8 encoding and `,` separator.

- Before execution, the script **aborts** if any file is already present in the output folder to avoid duplicate appends.

- This approach avoids a full in-memory dataframe by writing directly to CSV buffers, making it suitable for large-scale data.

Here's the Markdown documentation block for `~/notebooks/14_gen_filtered_work_data.ipynb` in the style you requested, fully copy-paste ready:

### Filter and Merge Work–Topic–Author Data
This notebook (`~/notebooks/14_gen_filtered_work_data.ipynb`) generates a cleaned and enriched dataset of works by merging **topic assignment** and **author metadata**. It filters out noisy assignments and prepares the data for downstream use.

The final per-topic outputs are stored in:

```
data/filtered_works_data_by_topic_parquet/
````

Each file is named `{topic_id}.parquet` and contains all works **assigned to that topic**, with the list of valid topics (topic score ≥ 0.9, 1 to 3 per work) and the list of contributing authors.

#### Output structure

Each row in the output parquet files contains:

- `work_id`: ID of the publication
- `date`: Publication date (ISO format, sorted ascending)
- `topics`: List of topics assigned to the work (max 3, all above threshold)
- `authors`: List of author IDs associated with the work

#### Extraction logic

* Input files:

  * Topic-assignment tables from `data/works2topic_by_topic_parquet/`
  * Author–work mappings from `data/author2work_by_topic_parquet/`

* Steps:

  1. For each topic, load the score-based topic assignment file.
  2. Keep only the topic IDs with a score ≥ 0.9 (up to 3 per work).
  3. Construct a list of valid topics for each work.
  4. Rename and merge author–work tables (grouping authors by work ID).
  5. Join the topic and author tables by `work_id` and `date`.
  6. Filter out rows with empty topic lists.
  7. Convert `date` to datetime and sort rows chronologically.
  8. Save to `.parquet` with Brotli compression for fast downstream processing.

---

## Final Data Size (June 2025 Release – Processed)

This section reports the sizes of the main folders generated during processing of the June 2025 OpenAlex snapshot.  
Note: Some folders contain both `.csv` and `.parquet` formats, contributing to their combined size.

### Folder Sizes

| Size    | Folder                                  | Notes                                                                      |
|--------:|-----------------------------------------|----------------------------------------------------------------------------|
| 72G     | `works_by_topic_csv/`                   | Raw `.csv` version of all works by topic                                   |
| 23G     | `works_by_topic_parquet/`               | All works per primary topic (parquet)                                      |
| 19G     | `authors/`                              | Includes `authors.csv` (14G) and `.parquet`                                |
| 2.5M    | `funders/`                              | Contains both `funders.csv` (1.9M) and `.parquet`                          |
| 6.0M    | `topics/`                               | Contains `topics.csv` (4.9M) and `topics.parquet`                          |
| 14M     | `institutions/`                         | Includes `institutions.csv` (11M) and `.parquet`                           |
| 1.2G    | `all_works2primary_topic_parquet/`      | Flat table mapping each work to its primary topic                          |
| 5.7G    | `author2work_by_topic_parquet/`         | Author-to-work mapping per topic                                           |
| 1.5G    | `works2year_by_topic_parquet/`          | Work counts per year and topic                                             |
| 2.7G    | `works2topic_by_topic_parquet/`         | All topic assignments per work                                             |
| 143G    | `works2text_by_topic_csv/`              | Raw `.csv` version of title + abstract info                                |
| 50G     | `works2text_by_topic_parquet/`          | Title + abstract info per topic (parquet)                                  |
| 18G     | `works2references_by_topic_parquet/`    | Exploded work-to-reference links (parquet)                                 |
| 143G    | `works2citations_by_topic_csv/`         | Citations exploded and grouped by cited topic                              |
| 20G     | `works2citations_by_topic_parquet/`     | Final citation edges (by topic, parquet)                                   |
| 3.7G    | `filtered_works_data_by_topic_parquet/` | Filtered works with valid topics and authors (by topic, parquet)           |


After validating `.parquet` conversions, you may choose to delete the original `.csv` files to save space.

After eliminating all `.csv` files, the overall size of the repository is about 130G.
However, one could only create or retain fewer data, for example the `filtered_works_data_by_topic_parquet/` and other description files, saving plenty of disk space.

### Example: CSV vs. Parquet Sizes

Several folders contain both `.csv` and `.parquet` versions:

| Folder         | CSV Size | Parquet Size |
|----------------|----------|--------------|
| `topics/`      | 4.9M     | 1.2M         |
| `funders/`     | 1.9M     | 656K         |
| `institutions/`| 11M      | 2.9M         |
| `authors/`     | 14G      | 4.6G         |

### Dataset statistics
In total, the dataset created (running this repository in June 2025) contains the following number of elements.

| Entity        | Count         |
|---------------|---------------|
| Works         | 210,864,615   |
| Authors       | 103,480,180   |
| Institutions  | 114,883       |
| Funders       | 32,437        |
| Topics        | 4,516         |

### Topic Hierarchy

The 4,516 topics are organized into a 4-level hierarchy:

- **4 Domains**
- **26 Fields**
- **252 Subfields**
- **4,516 Topics**
