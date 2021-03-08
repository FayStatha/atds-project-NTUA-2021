# This script is used to take the first 100 rows of "movie_genres.scv"
# to another csv, named "movie-genres-100.csv" for the purposes of Part B

import pandas as pd

df = pd.read_csv("movie_genres.csv").iloc[:100,:]
df = df[:100][:]

df.to_csv('movie-genres-100.csv')
