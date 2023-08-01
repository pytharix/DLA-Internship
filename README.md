# DLA-Intership Program: Web Scraping and Data Dashboard with Apache Airflow and BigQuery

## Overview

This project aims to scrape data from two webpages, Tokopedia and Shopee, specifically targeting the store "Adidas Combat Sport". The scraped data will be used to create a dashboard, and the entire process is managed through Apache Airflow with data storage in BigQuery. The project is designed to be run using Docker Compose and includes a Dockerfile for building the necessary Docker image.

## Features

- Web scraping from Tokopedia and Shopee to collect data from the store "Adidas Combat Sport".
- Data storage and management in BigQuery to create a data dashboard.
- Automated data scraping and insertion with Apache Airflow.
- Dockerized setup for easy deployment and portability.

## Installation

### Docker Image - "airflow_more"

To run the project, you need to build the Docker image named "airflow_more" using the provided Dockerfile. The Docker image includes the necessary Python libraries for web scraping and other project dependencies.

### Docker Compose

The project uses Docker Compose for orchestrating the entire setup. The "docker-compose.yml" file defines the services and volumes needed for the project. Note that the "FileNeeded" volume blends with the local storage and provides essential credential files and flat files required by the apps.

## Usage

1. Build the Docker image "airflow_more" using the provided Dockerfile.
2. Set up the necessary credentials and flat files in the "FileNeeded" volume.
3. Use Docker Compose to run the entire project.
4. Apache Airflow will automate the data scraping process and insert the scraped data into BigQuery.
5. Access the dashboard created in BigQuery to visualize and analyze the data.
