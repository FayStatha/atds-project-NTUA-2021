# atds-NTUA-2021

A project for Advanced Topics in Database Systems course of ECE, NTUA for fall semester of academic year 2020-2021.

The main purpose of the project is to familirize the student with the use of [Apache Spark](https://spark.apache.org/) and [Apache Hadoop](https://hadoop.apache.org/).

## Team Members

- [Efstathia Statha](https://github.com/FayStatha), fay.statha@gmail.com

- [Eleni Oikonomou](https://github.com/EleniOik), email

## Assignement

The assignement language is Greek, because that is the main language of certain course. 

Because of that, I am trying to describe the tasks in this file.

### PART A

Part A is about writing queries using either RDD API, or Spark SQL, on this [dataset](). 

Spark SQL queries need to work both with .csv files and with .parquet files.

**mporw na ftiaksw ena table me ta queries sta agglika**

### PART B

PART B is about implementing repartition and broadcast join on RDD API, based on [A Comparison of Join Algorithms for Log Processing in
MapReduce](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.644.9902&rep=rep1&type=pdf). 

The pseudocode for repartition join is A.1. and for broadcast join in A.4. 

Furthermore, we had to make a change on some given code to test the performance of Spark SQL queries on parquet files with, or without, enabling the query optimazer to use broadcast join.
