#!/usr/bin/env python
# coding: utf-8

# # Technical test results for HEVA company

# This notebook repeats the statement of the test. Under each activity you will find the code and the result produced.
# You will find all the requirements to run this notebook in the requirements.md file.

# ## Configuration

# ### 1. Importing packages

# In[25]:


# Import necessary modules

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col


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


# - 1.2 How many different users are in the database?

# In[15]:


def activity_1_2(df_ratings):
    """Counting the number of distinct user id

    Args:
        df_ratings (Dataframe): Ratings Dataframe

    Return:
        int: Number of user id
    """

    return df_ratings        .select("user_id")        .distinct()        .count()


result_1_2 = activity_1_2(df_ratings)
print("There are", result_1_2, "user id in the database")


# - 1.3 What is the distribution of the notes provided?
#      **Bonus**: create a histogram.

# In[22]:


def activity_1_3(df_ratings):
    """ Display rating distribution histogramme
        Counting the number of voters per rating
        Sorting based on ratings

    Args:
        df_ratings (Dataframe): Ratings Dataframe

    """

    # Creation of the histogram
    print("Converting Dataframe to Pandas...")
    plt.hist(
        df_ratings.select("rating").toPandas().squeeze(),
        bins=11)  # [0 to 10] => 11 values
    plt.xlabel('Rating')
    plt.ylabel('Number of rating')
    plt.title('Histogram of rating')
    plt.show()

    print("Ratings distribution")
    df_ratings        .groupBy("rating")        .count()        .orderBy("rating")        .show()


activity_1_3(df_ratings)


# - 1.4 Finally, we want to obtain a table of frequencies to express the distribution of notes as a percentage.

# In[26]:


def activity_1_4(df_ratings):
    """ Added column count which represents the number of voters
        by notes.
        Added a percentage column,
        which is a transformation of the count column into a percentage.
        Selection of rating and percentage columns.
        Sort by rating column.

    Args:
        df_ratings (Dataframe): Rating Dataframe
    """

    df_ratings.groupBy("rating")        .count()        .withColumn(
            'percentage',
            (col("count")*100)/float(df_ratings.count()))\
        .select("rating", "percentage")\
        .orderBy("rating")\
        .show()


print("Ratings frequencies")
activity_1_4(df_ratings)


# ## Code quality check

# In[29]:


get_ipython().system('flake8-nb result.ipynb')


# ## Safe Notebook versioning

# In[31]:


get_ipython().system('jupyter nbconvert result.ipynb --to="python"')

