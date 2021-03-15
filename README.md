# atds-NTUA-2021

A project for Advanced Topics in Database Systems course of ECE, NTUA for fall semester of academic year 2020-2021.

The main purpose of the project is to familirize the student with the use of [Apache Spark](https://spark.apache.org/) and [Apache Hadoop](https://hadoop.apache.org/).

## Team Members

- [Efstathia Statha](https://github.com/FayStatha), fay.statha@gmail.com

- [Eleni Oikonomou](https://github.com/EleniOik), elecon16@gmail.com

## Assignement

The assignement's and report's language is Greek, because that is the main language of this course. 

Because of that, I am trying to describe the tasks in this file.

### PART A

Part A is about writing queries using either RDD API, or Spark SQL, on this [dataset](https://ntuagr-my.sharepoint.com/:u:/g/personal/el16190_ntua_gr/EediLa79Da9Kjck19kFo9k0BXlzzvpCrR2tJNv3JI3hL0w?e=KFaOdk). 

Spark SQL queries need to work both with .csv files and with .parquet files.

| Query | Description                                                                                                                                                                                                                                                                                                                                |
|-------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Q1    | Find for each year, from 2000 and later, the movie with the most profit. To calculate profit we use the formula: (cost - income)/cost. Movies with no release date, or 0 to cost or income are excluded.                                                                                                                                   |
| Q2    | Find the percentage of users with average rating given to movies greater than 3.                                                                                                                                                                                                                                                           |
| Q3    | For each movie genre find the average movie rating and the number of movies that belongs to it.                                                                                                                                                                                                                                            |
| Q4    | For movies that belong to category 'Drama' find the average summary length (words) for each one of the four quinquenniums ('2000-2004', '2005-2009', '2010-2014', '2015-2019').                                                                                                                                                            |
| Q5    | For each movie genre find the user that has given the most ratings to movies that belongs in this genre. Also, find the number of those ratings and the user's most and least beloved movie of the genre, using his ratings. If the user has given the same rating to more than one movies, the most popular of them needs to be selected. |

### PART B

PART B is about implementing repartition and broadcast join on RDD API, based on [A Comparison of Join Algorithms for Log Processing in
MapReduce](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.644.9902&rep=rep1&type=pdf). 

The pseudocode for repartition join is A.1. and for broadcast join in A.4. 

Furthermore, we had to make a change on some given code to test the performance of Spark SQL queries on parquet files with, or without, enabling the query optimizer to use broadcast join.
