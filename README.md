# Data_Engineering
This is a repository for projects for IBM data engineering course

## Practice etl_pracitce_GDP.py

An international firm that is looking to expand its business in different countries across the world has recruited you. You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

You can find the required data on this webpage: https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29

The required information needs to be made accessible as a JSON file 'Countries_by_GDP.json' as well as a table 'Countries_by_GDP' in a database file 'World_Economies.db' with attributes 'Country' and 'GDP_USD_billion.'

Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. Also, log the entire process of execution in a file named 'etl_project_log.txt'.

You must create a Python code 'etl_project_gdp.py' that performs all the required tasks.

## Project

A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements.

Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate information made available to you as a CSV file. You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

1.  Write a function to extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a data frame.
2.  Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
3.  Write a function to load the transformed data frame to an output CSV file.
4.  Write a function to load the transformed data frame to an SQL database server as a table.
5.  Write a function to run queries on the database table.
6.  Run the following queries on the database table:
7.  a. Extract the information for the London office, that is Name and MC_GBP_Billion
8.  b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion
9.  c. Extract the information for New Delhi office, that is Name and MC_INR_Billion
10.  Write a function to log the progress of the code.
11.  While executing the data initialization commands and function calls, maintain appropriate log entries.
