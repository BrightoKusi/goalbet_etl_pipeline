# goalbet_etl_pipeline

# Introduction

Goalbet is a sport data analytics company that analyzes hsitoric sport data. We build predictive models to predict sport scores for insight analysis and betting purpose. We retrieve data from a wide variety of sources in a variety of formats.
As our newly hired data engineer, you're requested to :
1. Build an end-to-end Extract, Transform, and Load (ETL) pipeline that pulls data from the website of one of our data providers. (https://www.football-data.co.uk/englandm.php)
2. Use your file explorer as the data lake for staging the extracted raw and transformed data in csv format.
3. Use Postgresql for persisting the transformed data.

We are only interested in a subset of the following data:
Div, Date, Time, HomeTeam, AwayTeam, FTGH, FTAH

# Tools and libraries
- Python (version X.X)
- Pandas
- SQLAlchemy

# Processes
1. Download the datasets from the website and store on your local.
2. Read the files using Pandas
3. Transform the files by slicing and retaining required columns
4. Export files to Postgresql  





