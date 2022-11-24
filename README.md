# CDD-Java

Config Driven Development - Template to explain CDD approach, effeciently and implement it using different levels of CDD 

Problem statement: Read files from GCS and write file data to BQ.Function to read file and write data using hard coded connection params

This can achieved through different CDD levels. Each level adding more features of implementation

CDD0 - Hardcoded appraoch without using any configuration

CDD1- Added a config file and reading parameters from config to address usecase. Read CSV file from GCS and write to BQ

CDD2- Added more source and target options in config. Read CSV and Parquet files from GCS and write to BQ or Oracle 

CDD3- Added multiple cloud sources to read and write files to cloud and RDBMS. Read from GCS/ AWS cloud and write to Oracle/Cloud storage
