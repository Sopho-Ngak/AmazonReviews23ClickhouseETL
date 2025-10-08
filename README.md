# Amazon Reviews ETL Pipeline with ClickHouse

## Table of Contents
- [Setup Overview](#setup-overview)
- [Tasks](#tasks)
- [Deliverables](#deliverables)
- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Instructions to Run the Project](#instructions-to-run-the-project)
- [Usage](#usage)
- [Automation Challenge proposal](#automation-challenge-proposal)
- [Contributors](#contributors)


## Setup Overview
This repository contains a project for a Senior Data Engineer take-home assignment. The project is structured to demonstrate data ingestion, processing, and analysis using Python and ClickHouse.


## Tasks
1. Data Ingestion:
* Define a schema called amazon in ClickHouse to store the Amazon reviews.
* Create a ClickHouse table called reviews with appropriate columns and data types
matching the dataset fields – for instance: user_id (STRING), helpful_votes (INT),
etc. Use a suitable engine for the table to enable fast analytics. Consider order-
ing/primary key that could benefit your queries. For details about the columns,
check the data fields section here.
* Write a Python script that reads the review data in batches and inserts it
into the reviews table. The script should ensure that no duplicates are inserted.
The script should do a concurrency test before inserting on what observations have
already been inserted.Your keys should help a lot here. Make sure to use the
dbutils library for database connectivity and writing/ingesting to the database.
* Add logging to your script:
- Log when ingestion starts and finishes
- Log each batch processed (e.g. number of rows inserted)
- Log any errors encountered during ingestion

2. Analysis & Visualization (SQL + Polars):
* Using the dbutils package to fetch data, perform EDA to extract relevant aggregates, distributions, and patterns.
* Leverage Polars to further analyze and visualize the results.
* Identify meaningful insights in the reviews data and present them clearly with code and visualizations.

3. Automation Challenge:
* Describe how you would automate ingestion by detecting new files in a local `data/` folder and importing them into ClickHouse without duplication.
* This is a conceptual task – no need to implement it, but clearly explain the approach (tools/technologies or pseudocode) you would use to achieve automated data ingestion for continuously arriving files.

## Deliverables
* Code: Python scripts/notebooks for data ingestion and analysis (well-documented and
formatted).
* SQL Queries: A list of all SQL queries used for data exploration (can be included in
the Python script or a separate .sql file).
*  A small presentation of the insights from the analysis you did in task 2. This could be
in any format you chose. e.g. PDF, Power Point, etc.
* Automation Write-Up: A brief write-up detailing your ingestion automation approach
(from Task 3). This can be in Markdown, plain text, PDF, or any other readable
format — whichever you’re most comfortable with.
* README: A clear README file with instructions to set up the environment (Docker
services, virtual environment, etc.), run the ingestion script, execute analyses, and view
the results. Include any assumptions and how to reproduce your work.

## Requirements
| Requirement | Version |
|-------------|---------|
| Python      | >=3.10  |
| Docker      | >=20.10 |
| python3-pip | >=20.0  |
| dbutils     | =0.1.4  |
| git         | >=2.25  |

## Project Structure
```
AmazonReviews23ClickhouseETL/
│── config/
│   └── config.py          # Configuration file for credentials and settings
│   └── docs/             # Folder for project documentation
│       └── Automation Challenge.md  # Write-up for automation challenge
│── src/
│   ├── data/
│   │   └── analysis_outputs/  # Folder to save analysis outputs
│   ├── pipelines/
│   │   └── ingest.py       # Data ingestion script
│   │   └── analysis.py     # Data analysis script
│   ├── sql/
│   │   └── create_schema.py # SQL schema creation scripts
│   └── utils/
│       └── clickhouse.py  # ClickHouse database utility functions
│── .gitignore
│── docker-compose.yml      # Docker Compose file for setting up ClickHouse
│── main.py                 # Main script to run the project
│── README.md               # Project documentation
│── requirements.txt        # Python dependencies
```

## Instructions to Run the Project
To setup the project on local machine, you need to follow these steps:
1. Ensure `python3.10` is install in your machine and create a virtual environment and activate using the following command and activate it:
```bash
python3.10 -m venv venv
source venv/bin/activate
```
2. Install the required Python packages using pip:
- This project depends on the `dbutils` package for database connectivity. clone the repository from GitHub somewhere in your laptop and navigate to the cloned directory. Run the following commands:
```bash
git clone https://github.com/datasciencenbr/python-dbutils.git
cd python-dbutils
pip install .
```
- Navigate back to this project (`AmazonReviews23ClickhouseETL`) directory and install other dependencies listed in `requirements.txt`:
```bash
cd AmazonReviews23ClickhouseETL
pip install -r requirements.txt
```
3. Use the template in .env.example to create a .env file in the root directory of the project and update it with your ClickHouse credentials.

4. Start ClickHouse using Docker Compose:
```bash
docker-compose up -d
```
- wait for a few seconds to ensure ClickHouse is fully started. Then run this command to see in clickhouse is accessible outside the container:
```bash
curl "http://localhost:8123/"
```
- You should see a response like `Ok.\n` indicating that ClickHouse is running.
- You can now access clickhouse web interface at [`http://localhost:8123/play`](http://localhost:8123/play) using the  username and password in your `.env` file.


## Usage
Before running the scripts, ensure that the ClickHouse server is up and running and you have added at least one zipped json line file (e.g `file_name.jsonl.gz`) in the `src/data` folder.
In the root directory of the prroject there is a file called `main.py` which you can use to run the ingestion and analysis scripts. To run this file one command line arguments is required (commands accepted from this list [`ingest`, `generate_report`]) to specify which script to run and another command is optional to specify the data folder (default is `./src/data`).
1. First you need to run the ingestion script to load data into ClickHouse:
- Running from the default data folder `./src/data`:
```bash
python main.py ingest
```
- Running from a custom data folder:
```bash
python main.py ingest --data_folder ./path/to/your/data/folder
```
2. After ingestion is complete, you can run the analysis script to generate insights and visualizations stored in `src/data/analysis_outputs` as PNG files:
- Running from the default data folder `./src/data`:
```bash
python main.py generate_report
```
- Running from a custom data folder:
```bash
python main.py generate_report --data_folder ./path/to/your/data/folder
```

# Automation Challenge proposal
The detailed proposal for automating ingestion is available in the `docs/Automation Challenge.md` file.

NOTE: Run the ingestion script first before running the analysis script to ensure the table is created and there is data in the ClickHouse database. The analysis script depends on the data ingested by the ingestion script. The scripts will throw errors if the table needed for analysis is not found in the database.
Thanks for reviewing my submission. I look forward to your feedback!

### Contributors
[Sophonie Ngakoutou](https://github.com/Sopho-Ngak)
