#!/usr/bin/env python
# coding: utf-8

# # Technical test results for HEVA company

# This notebook repeats the statement of the test. Under each activity you will find the code and the result produced.
# You will find all the requirements to run this notebook in the requirements.md file.

# ## Configuration

# ### 1. Importing packages

# In[14]:


# Import necessary modules

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, when, explode, split,    desc, from_unixtime, year
from pyspark.sql.types import DateType


# ### 2. Settings

# In[3]:


# Definition of necessary parameters
data_path = "../sources/data/movies.sqlite"


# ### 3. Reading data

# In[4]:


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

# In[5]:


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

# In[6]:


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

# In[7]:


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

# In[8]:


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

# In[9]:


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


# ### 2. Data selection and enrichment
# 
# - 2.1 In order to set up a certain statistical model, we must transform the `rating` note into two modalities: did the user like the film or not?
#      Create a new `liked` column in the `ratings` table with the following values: `0` for ratings [0-6] and `1` for ratings [7-10].

# In[10]:


def activity_2_1(df_ratings):
    """ Added a liked column.
        Depending on the rating column,
        the liked column takes the value 0 or 1

    Args:
        df_ratings (Dataframe): Ratings Dataframe

    Returns:
        Dataframe: Updated ratings Dataframe
    """

    df_ratings = df_ratings        .withColumn(
            'liked',
            when(df_ratings.rating < 7, 0)
            .when(df_ratings.rating >= 7, 1))

    df_ratings.show()

    return df_ratings


print("Updated ratings Dataframe")
df_ratings = activity_2_1(df_ratings)


# - 2.2 Which genres are rated highest by users? We want to get the **top 10** movie genres liked by users (using the new `liked` column).

# In[11]:


def activity_2_2(df_movies, df_ratings):
    """ Separation of genres in an array with the split function.
        Extract genre arrays with the explode function alias explode_genre.
        Selection of the movie_id and explode_genre column.
        Joining with ratings table on movie_id columns.
        Sum of the liked column by grouping on the explode_genre column.
        Rename sum(liked) column to sum_liked.
        Rename explode_genre column to genre.
        Sort in descending order based on the sum_liked column.
        Limitation to the first 10 records.

    Args:
        df_movies (Dataframe): Movies Dataframe
        df_ratings (Dataframe): Ratings Dataframe
    """

    df_movies.select(
        "movie_id",
        explode(
            split(
                col("genre"),
                "\|"))
        .alias("explode_genre"))\
        .join(
            df_ratings,
            df_ratings.movie_id == df_movies.movie_id,
            "inner")\
        .groupBy("explode_genre")\
        .sum("liked")\
        .withColumnRenamed("sum(liked)", "sum_liked")\
        .withColumnRenamed("explode_genre", "genre")\
        .sort(desc("sum_liked"))\
        .limit(10)\
        .show()


print("Top 10 genres")
activity_2_2(df_movies, df_ratings)


# ### 3. Advanced Selections
# 
# - 3.1 What are the titles of the films most popular with Internet users?
#      We are looking for the **10** films with the best ratings on average by users, with a minimum of **5** ratings for the measurement to be relevant.

# In[12]:


def activity_3_1(df_movies, df_ratings):
    """ Join between movies and ratings tables,
        on movie_id columns, alias movies_ratings.
        Join with subtable alias title_count,
        which represents the number of votes per film.
        Filter on movies that have at least 5 ratings.
        Average ratings per movie title.
        Renamed avg(rating) column to mean_rating.
        Descending sort based on mean_rating column.
        Limitation to the first 10 records.

    Args:
        df_movies (Dataframe): Movies Dataframe
        df_ratings (Dataframe): Ratings Dataframe
    """

    df_movies.join(
        df_ratings,
        df_movies.movie_id == df_ratings.movie_id,
        "inner").alias("movies_ratings")\
        .join(
            (df_movies.join(
                df_ratings,
                df_movies.movie_id == df_ratings.movie_id,
                "inner")
                .groupBy("title")
                .count()).alias("title_count"),
            col("movies_ratings.title") == col("title_count.title"),
            "inner")\
        .filter(col("count") >= 5)\
        .groupBy("movies_ratings.title")\
        .mean("rating")\
        .withColumnRenamed("avg(rating)", "mean_rating")\
        .sort(desc("mean_rating"))\
        .limit(10)\
        .show()


print("Top 10 movies")
activity_3_1(df_movies, df_ratings)


# - 3.2 What is the most rated film in 2020?
#      **Note**: the `rating_timestamp` column is provided in the database as [Unix time](https://fr.wikipedia.org/wiki/Heure_Unix).

# In[15]:


def activity_3_2(df_movies, df_ratings):
    """ Adding a rating_year column,
        which corresponds to the year in which the vote was recorded.
        Join movies and ratings tables on movie_id columns.
        Counting the number of votes per film title.
        Sort descending in order of count.
        Rename column count to rating_count.
        Limitation to the first record.

    Args:
        df_movies (Dataframe): Movies Dataframe
        df_ratings (Dataframe): Ratings Dataframe
    """

    df_ratings        .withColumn(
            'rating_year',
            year(
                from_unixtime('rating_timestamp')
                .cast(DateType())))\
        .join(
            df_movies,
            df_ratings.movie_id == df_movies.movie_id)\
        .filter(col("rating_year") == 2020)\
        .groupBy("title")\
        .count()\
        .sort(desc("count"))\
        .withColumnRenamed("count", "rating_count")\
        .limit(1)\
        .show()


print("Best film of the year 2020")
activity_3_2(df_movies, df_ratings)


# ## Code quality check

# In[16]:


get_ipython().system('flake8-nb result.ipynb')


# ## Safe Notebook versioning

# In[ ]:


get_ipython().system('jupyter nbconvert result.ipynb --to="python"')

