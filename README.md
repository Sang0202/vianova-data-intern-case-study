# Case study


We want to know the __countries that don't host a megapolis__


The purpose of this exercice is to evaluate your skills in Python and SQL. You'll have to fork this repository and write a program that fetch the [dataset of the population of all cities in the world](https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/export/?disjunctive.cou_name_en), stores it in a database, then perform a query that will compute: what are the countries that don't host a megapoliss (a city of more than 10,000,000 inhabitants)? 

The program will save the result (country code and country name) as a tabulated separated value file, ordered by country name. 

You should answer as if you were writting production code within your team. You can imagine that the program will be run automatically every week to update the resulting data.

Please send us the link to your github repository with the answer of the exercise. 

# Overview
1. Fetches data from the provided API endpoint.
2. Creates a MySQL database to store the data fetched.
3. Imports data to MySQL database.
4. Perform a query to compute the result and saves it.
5. Exports the results to tsv file.

We can run automatically this program every week by setting a new Task Scheduler task on Windows or cron job on Linux.
We can also schedule a task using Apache Airflow to update local MySQL database
