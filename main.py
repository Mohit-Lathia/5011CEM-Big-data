import dask.dataframe as dd
import numpy as np
import pandas as pd
import dask.array as da
import dask.bag as db
from scipy import stats
import datetime
import plotly.express as px
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from multiprocessing import Process, freeze_support, set_start_method
import multiprocessing
from functools import partial
import time 
import os

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section A
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def question1a():

    plot1 = time.time() # check the time for the modle to be produced 

    # filter the dataset for national level data and remove unnecessary columns
    stay_at_home = big_data[(big_data.Level == "National")]
    stay_at_home = stay_at_home.drop(columns=['State FIPS', 'State Postal Code', 'County FIPS', 'County Name'])
    stay_at_home["Week"] = dd.to_datetime(stay_at_home["Week"]) # Convert 'Week' column to datetime format

    # extract a subset of data from 'stay_at_home' that are within the range of dates in 'small_data'
    small_set = stay_at_home[stay_at_home["Date"].between(small_data['Date'].min(), small_data['Date'].max())].compute()

    # Extract the 'Population Staying at Home' values from the subset
    data_to_plot = small_set['Population Staying at Home'].values  
    data_to_plot = np.array(data_to_plot)

    # plots the histogram
    plt.figure(figsize=(10, 7))
    plt.title("A Histogram to show People Staying at Home")
    plt.xlabel("People Staying at Home")
    plt.ylabel("frequncys")
    plt.hist(data_to_plot, bins=[5000000 * i for i in range(8,26)])  # plots the histogram automactly 
    plt.grid(True)
    time_taken = time.time() - plot1 # takes the time taken for the graph to be made
    print("question1a-Plot 1",time_taken) # show the time 
    #plt.show() # shows the hisstogam 


    plot2 = time.time()

    # dictionary to store average number of trips
    avg_num = {}
    # list of column names
    columns_name = ["Trips <1 Mile", "Trips 1-3 Miles", "Trips 3-5 Miles",
        "Trips 5-10 Miles", "Trips 10-25 Miles", "Trips 25-50 Miles",
        "Trips 50-100 Miles", "Trips 100-250 Miles", "Trips 250-500 Miles",
        "Trips 500+ Miles"]
    
    # loop through each column to calculate and store the average
    for columns in columns_name:
        avg_num[columns] = small_data[columns].mean().compute()

    # extracting keys and values
    distance = list(avg_num.keys())
    avrage = list(avg_num.values())

    # Plot the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(distance, avrage)
    plt.title("A bar chart to show a Distribution of Number of Trips vs People travling")
    plt.xlabel("Number of Trips")
    plt.ylabel("people travling")
    plt.grid(True)
    time_taken = time.time() - plot2
    print("question1a-Plot 2",time_taken)
    plt.show()


# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section B
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# plots 10,00000 thing
def question1b():
    plot1 = time.time()

    trips = big_data[(big_data.Level == "National")]
    trips = trips.drop(columns=['State FIPS', 'State Postal Code', 'County FIPS', 'County Name'])
    trips["Week"] = dd.to_datetime(trips["Week"])


    df_10_25 = trips[trips['Number of Trips 10-25'] > 10000000]
    
    plt.scatter(np.array(df_10_25["Date"]), df_10_25['Number of Trips 10-25'],alpha= 0.5)
    plt.xticks(visible=False)
    plt.tick_params(axis=u'both', which=u'both',length=0)
    plt.title("A scater graph to show Dates when >10,000,000 people took 10-25 trips")
    plt.xlabel("Date")
    plt.ylabel("Number of Trips")
    time_taken = time.time() - plot1
    print("question1b-Plot 1",time_taken)
    plt.show()

    plot2 = time.time()
    df_50_100 = trips[trips['Number of Trips 50-100'] > 10000000]
    
    plt.scatter(np.array(df_50_100["Date"]), np.array(df_50_100['Number of Trips 50-100']), alpha= 0.5)
    plt.xticks(visible=False)
    plt.tick_params(axis=u'both', which=u'both',length=0)
    plt.title("A scater graph to show Dates when >10,000,000 people took 50-100 trips")
    plt.xlabel("Date")
    plt.ylabel("Number of Trips")
    time_taken = time.time() - plot2
    print("question1b-Plot 2",time_taken)
    plt.show()

   
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section c
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Runs paraless prosecting
def processor_time(): 
    processor10_20 = [1] # define the number of processors
    time_process = {} # create an empty dictionary to store time taken

    for processor in processor10_20: # loop through each processor configuration
        print(f"Starting computation with {processor} processors...")
        setup = time.time() 
        client = Client(n_workers=processor) # initialize a Dask client with the specified number of workers

        time_taken_setup = time.time() - setup
        print(f"Time taken for setup:{time_taken_setup} Seconds")


        # example function calls:
        question1a()
        question1b()
        leaner_regration(big_data, small_data)
        question1e()

        time_taken = time.time() - setup # calculate the time taken

        time_process[processor] = time_taken # store the time taken in the dictionary

        print(f"Time with {processor} processors: {time_taken} seconds")

        # close the client after computation
        client.close()

    print(time_process) # prints the overall time

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# section D
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def leaner_regration(big_data, small_data):

    plot1 = time.time()

    national = big_data[(big_data["Level"] == "National") & (big_data["Week"] == 31) & (big_data["Date"].str.endswith("2019"))]
    
    y = np.array(national['Number of Trips 10-25'])

    x = np.array(small_data['Trips 10-25 Miles'])
 
    slope, intercept, r, p, std_err = stats.linregress(x, y)


    plt.scatter(x, y, alpha=0.5)
    plt.axline((0, intercept), slope = slope)
    plt.title("A scater graph to show 'Tips 20-100 miles' vs 'Number of Trips 10-25'")
    plt.xlabel("Trips 25-100 Miles")
    plt.ylabel("Number of Trips 10-25")
    time_taken = time.time() - plot1
    print("question1d - Plot 1",time_taken)
    plt.show()
    
#------------------------------------------------------------------------------------------------------------------------
# section e
#------------------------------------------------------------------------------------------------------------------------------
def question1e():

    plot1 = time.time()

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
    plt.ylabel("people travling")
    plt.grid(True)
    time_taken = time.time() - plot1
    print("question1e-Plot 1",time_taken)
    plt.show()
#-----------------------------------------------------------------------------------------------------------------------------------------
# end  
#-------------------------------------------------------------------------------------------------------------------------------------------

# runs the functions 
if __name__ == '__main__':
    # reads the csv file called 'trips_by_distance'
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

# reads the csv file called 'trrips_ful_data'
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

    freeze_support()
    multiprocessing.freeze_support()
    processor_time()
    