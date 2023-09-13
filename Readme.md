# Airbnb Boston Data Analysis
This project aims to analyze the Boston Airbnb dataset using PySpark. The dataset contains three main dataframes: listings_df, calendar_df, and reviews_df. Each dataframe provides valuable information about Airbnb listings, their availability, and guest reviews in Boston.

## Prerequisites
Before running the PySpark code, ensure that you have the following prerequisites installed:

1. Apache Spark
2. PySpark
3. Jupyter Notebook (optional)

You can install PySpark and set up your environment using the official [PySpark documentation](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)

## Data Description

* A sneak peek into the Airbnb activity in Boston, MA, USA.
    * This dataset contains 3 csv files which are given below:
        + Listings, including full descriptions of the Airbnb properties
        + Reviews, including unique id for each reviewer and detailed comments
        + Calendar, including listing id and the price and availability for that day
* The common key for all 3 dataset is the listing_id which is the unique identifier assigned to each listing
* The [link](https://www.kaggle.com/datasets/airbnb/boston?select=calendar.csv) to the dataset used.

# Schema

![Schema representation](/image/miniprojectschema.jpg)

# Questions:

## 1. Host Portfolio Growth Analysis: Identify hosts who have significantly grown their portfolio over time by calculating the percentage increase in the number of listings they manage. Analyze any common characteristics among these hosts.

## Insights:
This code aims to identify hosts who have significantly expanded their portfolio of listings over time and extract common characteristics among them. The "portfolio_growth" metric is used to measure this growth, and hosts exceeding the specified threshold can be considered for further analysis. The resulting Data Frame provides insights into the characteristics of hosts with substantial portfolio growth.

## Output:
![Output for Question 1](/image/output1.png)

## 2. Calculate the booking rate (percentage of available dates booked) for each listing, considering the day of the week. Identify listings with significant booking rate fluctuations based on the day of the week. Also, display the day with highest average booking rate and the create a pivot that shows days of a week and the average booking rates for each.

## Insights:
You can gain insights into how different listings are booked on different days of the week, identify listings with booking rate fluctuations, and understand if there are particular days that tend to have higher average booking rates. This information can be valuable for property management and marketing strategies.

## Output:
![Output for Question 2.1](/image/output2_1.png)

![Output for Question 2.2](/image/output2_2.png)

# Running the Code
To run the PySpark code and explore the analysis, you can use the provided Jupyter Notebook files in the notebooks/ directory. Make sure to adjust the file paths according to your local setup.

# Acknowledgments
Airbnb for providing the dataset.
Apache Spark and PySpark community for powerful data analysis tools.




