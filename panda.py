import pandas as pd # calls the panda module
import dask.dataframe as dd # calls the dask modul 
import time # call a time module

# how long panda took
start = time.time() # start the timer 
a = pd.read_csv('trips_by_distance.csv') # reads the database 
end = time.time() # ends the timer 
timer = end - start # gets the result 
print("panda results: ") 
print(timer) # prints the result 

# how long dask took
start = time.time()  # start the timer 
a = dd.read_csv('trips_by_distance.csv') # reads the database 
end = time.time() # ends the timer 
timer = end - start # gets the result
print("dask results: ")
print(timer) # prints the result 