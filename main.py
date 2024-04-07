import dask.dataframe as dd
import numpy as np
import pandas as pd
import dask.array as da
import dask.bag as db
import datetime
import plotly.express as px
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from dask.distributed import Client, LocalCluster
from multiprocessing import Process, freeze_support, set_start_method
import multiprocessing
from functools import partial
import time 
import os


big_data = dd.read_csv('trips_by_distance.csv', dtype={
    "Level": "string",
    "Date": "string",
    "State": "string",
    "FIPS": "float64",
    "State Postal Code": "string",
    "County FIPS": "float64",
    "County Name": "string",
    "Population Staying at Home": "float64",
    "Population Not Staying at Home": "float64",
    "Number of Trips": "float64",
    "Number of Trips <1": "float64",
    "Number of Trips 1-3": "float64",
    "Number of Trips 3-5": "float64",
    "Number of Trips 5-10": "float64",
    "Number of Trips 10-25": "float64",
    "Number of Trips 25-50": "float64",
    "Number of Trips 50-100": "float64",
    "Number of Trips 100-250": "float64",
    "Number of Trips 250-500": "float64",
    "Number of Trips >=500": "float64",
    "Row ID": "string",
    "Week": "float64",
    "Month": "float64"
})

small_data = dd.read_csv('trips_full_data.csv', dtype={
    "Months of Date": "string",
    "Week of Date": "string",
    "Year of Date": "float64",
    "Level": "string",
    "Date": "string",
    "Week Ending Date": "string",
    "Trips <1 Mile": "float64",
    "People Not Staying at Home": "float64",
    "Trips": "float64",
    "Trips 1-25 Miles": "float64",
    "Trips 1-3 Miles": "float64",
    "Trips 10-25 Miles": "float64",
    "Trips 100-250 Miles": "float64",
    "Trips 100+ Miles": "float64",
    "Trips 25-100 Miles": "float64",
    "Trips 25-50 Miles": "float64",
    "Trips 250-500 Miles": "float64",
    "Trips 3-5 Miles": "float64",
    "Trips 5-10 Miles": "float64",
    "Trips 50-100 Miles": "float64",
    "Trips 500+ Miles": "float64"
})

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section A
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def question1a():
    stay_at_home = big_data[(big_data.Level == "National")]
    stay_at_home = stay_at_home.drop(columns=['State FIPS', 'State Postal Code', 'County FIPS', 'County Name'])
    stay_at_home["Week"] = dd.to_datetime(stay_at_home["Week"])

    small_set = stay_at_home[stay_at_home["Date"].between(small_data['Date'].min(), small_data['Date'].max())].compute()

    # Assuming you have computed data to plot here
    data_to_plot = small_set['Population Staying at Home'].values  # Selecting the column and converting to array

    # Pass data to grapg_question1 function to plot histogram
    grapg_question1(data_to_plot)

# plots and shows the graph
def grapg_question1(date):
    plt.figure(figsize=(10, 7))
    plt.title("A Histogram to show People Staying at Home Distribution")
    plt.xlabel("People Staying at Home")
    plt.ylabel("Frequency")
    plt.hist(date, bins= "auto")  # plots the histogram
    plt.grid(True)
    plt.show()  # shows the histogram


def pt2_question1a():
    # Select the columns representing different trip distances
    
    avg_num = {}
    columns_name = ["Trips <1 Mile", "Trips 1-3 Miles", "Trips 3-5 Miles",
        "Trips 5-10 Miles", "Trips 10-25 Miles", "Trips 25-50 Miles",
        "Trips 50-100 Miles", "Trips 100-250 Miles", "Trips 250-500 Miles",
        "Trips 500+ Miles"]
    

    for columns in columns_name:
        avg_num[columns] = small_data[columns].mean().compute()

    distance = list(avg_num.keys())
    avrage = list(avg_num.values())

    # Plot the histogram
    plt.figure(figsize=(10, 6))
    plt.bar(distance, avrage)
    plt.title("A Histohram to show a Distribution of People Traveling vs Distance")
    plt.xlabel("Number of Trips")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.show()

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section B
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# plots 10,00000 thing
def question1b():

    trips = big_data[(big_data.Level == "National")]
    trips = trips.drop(columns=['State FIPS', 'State Postal Code', 'County FIPS', 'County Name'])
    trips["Week"] = dd.to_datetime(trips["Week"])


    df_10_25 = trips[trips['Number of Trips 10-25'] > 10000000]
    
    plt.scatter(df_10_25["Date"], df_10_25['Number of Trips 10-25'], alpha= 0.5)
    plt.title("Dates when >10,000,000 people took 10-25 trips")
    plt.xlabel("Date")
    plt.ylabel("Number of Trips")
    plt.show()

    df_50_100 = trips[trips['Number of Trips 50-100'] > 10000000]
    
    plt.scatter(df_50_100["Date"], df_50_100['Number of Trips 50-100'], alpha= 0.5)
    plt.title("Dates when >10,000,000 people took 50-100 trips")
    plt.xlabel("Date")
    plt.ylabel("Number of Trips")
    plt.show()

   
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section c
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Runs paraless prosecting
def processor_time():
    processor10_20 = [10, 20]
    time_process = {}

    for processor in processor10_20:
        print(f"Starting computation with {processor} processors...")
        client = Client(n_workers=processor)

        start = time.time()

        # Call your functions without arguments if they don't need any
        question1a()
        pt2_question1a()
        question1b()

        time_taken = time.time() - start

        time_process[processor] = time_taken

        print(f"Time with {processor} processors: {time_taken} seconds")

        # Close the client after computation
        client.close()

    print(time_process)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section D
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    freeze_support()
    multiprocessing.freeze_support()
    processor_time()
    