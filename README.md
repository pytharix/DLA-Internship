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

# Web Scraping Steps

This section describes the step-by-step process of web scraping for both Tokopedia and Shopee, as well as extracting reviews from each website.

## Step 1: Scrape Tokopedia

1. The apps will start by scraping Tokopedia without requiring login.
2. The first step is to collect all store information, such as name, city, rate, etc.
3. After confirming the store information, the apps will proceed to collect all products listed under the category "Produk Terjual" (product sold page) for the store "Adidas Combat Sport".
4. The apps will then take the necessary information from the product page.
5. Next, the apps will visit each product page one by one to obtain detailed information about each product.
6. After scraping all the required data from Tokopedia, the apps will finish scraping for the first web.

Note: To change the store that the apps will scrape, modify the "store_list" variable in the file "ProductScrape.py" located in "DLA-Intership/Scrape". The URL of the store should be provided in the "store_list".

## Step 2: Scrape Shopee

1. Scraping Shopee requires login to access the page.
2. The login process is not fully automated for all users yet, and currently, the apps require input for username/phone number and password.
3. To input your credentials, find the lines written as driver_.find_element(By.NAME, "loginKey").send_keys("") and driver_.find_element(By.NAME, "password").send_keys("") in the code. Replace the empty strings with your username/phone number and password, respectively.
5. After login, the apps will collect all information of the store "Adidas Combat Sport".
6. Then, the apps will go to the category page specified in the code. The categories are defined in the "categ" variable, which can be found by examining the URL in Shopee.
7. The apps will collect all products and their information for each category.
8. It will then visit each product listed in that category to collect detailed information.
9. The process continues for all categories assigned before.
10. After scraping all required data from Shopee, the apps will finish scraping for the second web.

Note: To change the store that the apps will scrape, modify the "shopee_link" variable in the file "ProductScrape.py" located in "DLA-Intership/Scrape". The URL of the store should be provided in the "shopee_link".

## Step 3: Scrape Reviews

1. The apps to scrape reviews are in the file "ReviewScrape.py" located in "DLA-Intership/Scrape".
2. The "links" variable in lines 184 to 185 contains the URLs of the review pages for each web, with the first value for Tokopedia and the second value for Shopee.
3. Starting with Tokopedia, the apps will visit the review page and list the reviews left by customers on the first page (total of 10 reviews).
4. After collecting all reviews on that page, the apps will click the "next" button to move to the next page. The iteration for page numbers is set to 20, which means 20 pages will be scraped. You can change this iteration in line 112 as `for each_page in range(0, 19)`.
5. Once all pages are visited for Tokopedia, the apps will move to Shopee using the URL in the second value of the "links" list.
6. The process for scraping reviews on Shopee is similar to Tokopedia, with an iteration for page numbers set to 138 (line 146).

All collected data will be transformed into Python JSON objects and saved as flat file JSON format.

