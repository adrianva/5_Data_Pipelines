# 5_Data_Pipelines
This repository contains all the files for the Cloud Data Lake project of the Data Pipelines Nanodegree Program by Udacity.

## Introduction
"A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow."

## Usage
Assuming that Airflow and Redshift are up and running, we have to add two connections to Airflow:
1. aws_credentials: We need to provide the access key and the secret in order to connect with AWS.
2. redshift: Connection to the Redshift cluster

## Project Structure
* dag/udac_example_dag.py: Contains the tasks and dedependencies of the dag
* dag/sql/create_tables.py: Script that creates the model where we will load the data
* plugins/helpers/sql_queries.py: Queries that load the data into the model (fact and dimension tables)
* plugins/operators/data_quality.py: Operator that allows us to run queries in order to validate our data once the data has been loaded into the tables
* plugins/operators/load_dimension.py: Operator that loads data into some dimension table
* plugins/operators/load_fact.py: Operator that loads data into the fact table
* plugins/operators/stage_redshift.py: Operator that loads data from S3 into some staging table

## DAG Diagram
The final DAG looks like the followind image:
![Final DAG](/img/final_dag.png?raw=true "Final DAG")

I have added a new task called create_tables after the begin execution task just to make sure that all the tables are created correctly every time the DAG is run instead of doing this separately. That's why I also moved the create_tables.py script into the dag folder, because it is a requirement of the PostgresOperator when we want to run a file.
