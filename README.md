# DATA MODELLING WITH POSTGRES - Project 1 
This is a learning project with the aim to apply data modeling with Postgres and build an ETL pipeline using Python. In this project it is needed to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL. 

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Setup](#setup)
* [Usage](#usage)
* [Project Status](#project-status)
* [Room for Improvement](#room-for-improvement)
* [Acknowledgements](#acknowledgements)
* [Contact](#contact)
<!-- * [License](#license) -->


## General Information
This project is presented in a real case context where Sparkify, a music streaming firm, wants to assess the information they've gathered on songs and user activity on their new app. The music that customers are listening to is of special interest to the analytics team. However, they presently lack an easy way to query their data, which is kept in a directory of JSON logs on app user behavior as well as a directory of JSON metadata on the music in the app. 

The idea is to organize in a DB spase all the raw data that they have been collecting to create an easy an optimal way to access the data with simple queries. 


## Technologies Used
- Python - version 3.0

## Features
List the ready features here:
1. Star schema relationa DB based on 5 tables connected using primary and foraing keys.

This star schema is based on 4 dimensional tables (songs, artists, time and users) which have a unique identifier which is the primery key that will be linked to the fact table (songplays). This configuration is the most optimal for the raw data that we have, since there isn't a big amount of dimensions and we do not need 1-to-many connections. It allows a certain lever of denormalization wich makes the information more accessible. 

2. A script to set up the DB space (create_tables.py)
3. A script to extract, transform, load the raw data
4. A test script in a jupyter notebook to validate the creation of the DB space

## Setup
To run this project locally, I wold reccommend to create an environment with "virtual environment" 
Web to create a venv [_here_](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/).

Then, it will be needed to install these packages to make run the project.
- pip install postgres
- pip install os-sys
- pip install pip install pandas
- pip install 

## Usage
This project consists of 3 scripts and one for testing. 

1. sql_queries = is the script which collects all the sql queries that will be needed to drop tables, create tables, insert data and search for info inside specific tables.

2. create_tables = is the script in charge of creating the DB space and all the tables needed to organize the raw data. All the tables are connected between them with the use of primary and foreing keys. Therefore, all the IDs are unique for the different tables. 

3. etl = is the script where tha data is processed and introduced to the corresponding tables. 

To run the project it is needed to run this two statements in the terminal:

`python -m create_tables
python -m etl`


## Project Status
Project is: _complete_ 


## Room for Improvement

Examples of queries that we could create 

See if there are preferences on artists between girls and boys: 

> SELECT users.user_id, users.gender, artists.artist_id, artists.name FROM ((users JOIN songplays ON users.user_id = songplay.suser_id) JOIN artists ON songplay.artist_id = artists.artist_id)

## Acknowledgements
- Udacity team.
- colleges from the online course


## Contact
Created by [@maullaina] maullaina@gmail.com