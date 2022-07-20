#!/usr/bin/env python
# coding: utf-8

# # Technical test results for HEVA company

# This notebook repeats the statement of the test. Under each activity you will find the code and the result produced.
# You will find all the requirements to run this notebook in the requirements.md file.

# ## Configuration

# ### 1. Importing packages

# In[1]:


# Import necessary modules

from pyspark.sql import SparkSession


# ### 2. Settings

# In[7]:


# Definition of necessary parameters
data_path = "../sources/data/movies.sqlite"


# ### 3. Reading data

# In[9]:


def read_data(data_path):
    """ Configuring the Pyspark session with the jdbc package
        to read the "movies.sqlite" file.

    Args:
        data_path (string): The sqlite data file path

    Returns:
        tuple: A tuple of 2 Pyspark Dataframes
    """

    # Creation of the Spark session
    spark = SparkSession.builder        .config(
            'spark.jars.packages',
            'org.xerial:sqlite-jdbc:3.34.0')\
        .getOrCreate()

    # Reading the movies table
    df_movies = spark.read.format('jdbc')        .options(
            driver='org.sqlite.JDBC',
            dbtable='movies',
            url=f'jdbc:sqlite:{data_path}')\
        .load()

    # Reading the ratings table
    df_ratings = spark.read.format('jdbc')        .options(
            driver='org.sqlite.JDBC',
            dbtable='ratings',
            url=f'jdbc:sqlite:{data_path}')\
        .load()

    return df_movies, df_ratings


df_movies, df_ratings = read_data(data_path)


# ### 4. Data overview

# In[11]:


def preview_data(df_movies, df_ratings):
    """Showing top 20 rows

    Args:
        df_movies (Dataframe): Movies Dataframe
        df_ratings (Dataframe): Ratings Dataframe
    """

    # Overview of movies table data
    print("Movies table")
    df_movies.show()

    # Preview data from the ratings table
    print("Ratings table")
    df_ratings.show()


preview_data(df_movies, df_ratings)


# ## Tasks
# 
# ### 1. Counts
# 
# - 1.1 How many films are in the database?

# In[13]:


def activity_1_1(df_movies):
    """Counting the number of distinct film titles

    Args:
        df_movies (Dataframe): Movies Dataframe

    Return:
        int: Number of movies
    """

    return df_movies        .select("title")        .distinct()        .count()


result_1_1 = activity_1_1(df_movies)
print("There are", result_1_1, "movies in the database")


# ## Safe Notebook versioning

# In[14]:


get_ipython().system('jupyter nbconvert result.ipynb --to="python"')

