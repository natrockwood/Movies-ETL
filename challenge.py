# Import needed modules
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import sys
import time

# Create filepath.
file_dir = input(str("Copy and paste file path of where your data files are: "))

# Create password that will be used.
db_password = input(str("Enter postgres password: "))

# Input ETL script here:
# Try (Assmpt. 1): Not all modules are imported correctly.
try:
    # Create a function for 3 data sets
    def movies(wiki_movies,kaggle_metadata,ratings):
        # Try (Assmpt. 2): User filepath is wrong
        try:
            with open(wiki_movies, mode='r',encoding='utf8') as file:
                wiki_movies_raw = json.load(file)
            kaggle_metadata = pd.read_csv(kaggle_metadata)
            ratings = pd.read_csv(ratings)

        # Except (Assmpt. 2): Ask user to enter correct filepath
        except:
            print('Please enter correct filepath.')

        # Create a list of movies
        wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]

        # Create a dataframe for unclean wikipedia data:
        wiki_movies_df = pd.DataFrame(wiki_movies)

        def clean_movie(movie):
            movie = dict(movie) #create a non-destructive copy
            alt_titles = {}

            # Loop through a list of all alternative title keys.
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune–Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:

                # Check if the current key exists in the movie object.
                # If so, remove the key-value pair and add to the alternative titles dictionary.
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)

            # After looping through every key, add the alternative titles dict to the movie object
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles

            # merge column names
            def change_column_name(old_name, new_name):
                if old_name in movie:
                    movie[new_name] = movie.pop(old_name)
            change_column_name('Adaptation by', 'Writer(s)')
            change_column_name('Country of origin', 'Country')
            change_column_name('Directed by', 'Director')
            change_column_name('Distributed by', 'Distributor')
            change_column_name('Edited by', 'Editor(s)')
            change_column_name('Length', 'Running time')
            change_column_name('Original release', 'Release date')
            change_column_name('Music by', 'Composer(s)')
            change_column_name('Produced by', 'Producer(s)')
            change_column_name('Producer', 'Producer(s)')
            change_column_name('Productioncompanies ', 'Production company(s)')
            change_column_name('Productioncompany ', 'Production company(s)')
            change_column_name('Released', 'Release Date')
            change_column_name('Release Date', 'Release date')
            change_column_name('Screen story by', 'Writer(s)')
            change_column_name('Screenplay by', 'Writer(s)')
            change_column_name('Story by', 'Writer(s)')
            change_column_name('Theme music composer', 'Composer(s)')
            change_column_name('Written by', 'Writer(s)')

            return movie

        clean_movies = [clean_movie(movie) for movie in wiki_movies]

        # Create a dataframe for the clean movies:
        wiki_movies_df = pd.DataFrame(clean_movies)

        # Extract the link IDs
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

        # Remove duplicates from dataframe, count the rows
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

        # Determine columns to keep:
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]

        # Create a dataframe copy with the columns to keep
        wiki_movies_df2 = wiki_movies_df[wiki_columns_to_keep].copy()

        # Drop non-value data in the Box Office column
        box_office = wiki_movies_df2['Box office'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Use regex to format the box office column
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

        # Replace range with a dollar sign
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Create a function to turn the extracted values into a numeric value
        def parse_dollars(s):

            # if s is not a string, return NaN
            if type(s) != str:
                return np.nan

            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " million"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

                # convert to float and multiply by a million
                value = float(s) * 10**6

                # return value
                return value

            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

                # convert to float and multiply by a billion
                value = float(s) * 10**9

                # return value
                return value

            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

                # remove dollar sign and commas
                s = re.sub('\$|,','', s)

                # convert to float
                value = float(s)

                # return value
                return value

            # otherwise, return NaN
            else:
                return np.nan

        # Drop box office column
        wiki_movies_df2.drop('Box office', axis=1, inplace=True)

        # Replace the values in the box office column to match either form_one and form_two to replace the old Box Office column
        wiki_movies_df2['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Create a budget variable from the Budget column, removing null values, and converting values to strings
        budget = wiki_movies_df['Budget'].dropna().map(lambda x: ' '.join(x) if type(x) == list else x)

        # Use regex to remove values between dollar signs and hyphens
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Remove the citation references
        budget = budget.str.replace(r'\[\d+\]\s*', '')

        # Replace the values in the budget column to match either form_one and form_two to replace the old Budget column
        wiki_movies_df2['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Drop the original budget column
        wiki_movies_df2.drop('Budget', axis=1, inplace=True)

        # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
        release_date = wiki_movies_df2['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Make date forms
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'

        # Extract the dates
        release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

        # Create a function to parse the dates using to_Datetime
        wiki_movies_df2['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

        # Drop the original release_date column
        wiki_movies_df2.drop('Release date', axis=1, inplace=True)    

        # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
        running_time = wiki_movies_df2['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Extract running_time digits, and we want to allow for both possible formats
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m').apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

        # Apply a function that will convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero
        # Save the output to wiki_movies_df2
        wiki_movies_df2['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

        # Drop the original runnning time column
        wiki_movies_df2.drop('Running time', axis=1, inplace=True)

        # Drop adult movies in Kaggle Metadata
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

        # Try (Assmpt. 3): The datatypes can't be changed
        try:
            # Changing Kaggle metadata datatypes
            kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
            kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
            kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
            kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

        # Except (Assmpt. 3): TypeError, "Can't change the datatype."
        except TypeError:
            print("Can't change the data types.")

        # Change timestamp data to to_Datetime datatype
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

        # Try (Assmpt. 4): try merging and dropping columns
        try:
            # Merge Wiki dataframe and Kaggle dataframe
            movies_df = pd.merge(wiki_movies_df2, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

            # Dropping wiki data for title, release dates, language and production company columns
            movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
        # Except (Asspmt. 4): Print "Check merged columns."
        except:
            print("Merge did not work: check columns and dataframes to ensure merge worked correctly.")

        # Make a function that fills in missing data for a column pair and then drops the redundant column
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
                , axis=1)
            df.drop(columns=wiki_column, inplace=True)    

        # Run function for appropriate columns.
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

        # Reorder columns
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]

        # Rename the columns to be consistent
        movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)

        # Pivot this data so that movieId is the index, the columns will be all the rating values, and the rows will be the counts for each rating value
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')

        # Prepend rating_ to each column with a list comprehension to rename the columns so they’re easier to understand
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        # Merge dataframes
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

        # Fill in zeros
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

        # Try (Assmpt. 5): Engine module wasn't found
        try:
            # Create server and connecting string
            db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5432/movie_data"

            # Creating engine for database
            engine = create_engine(db_string)

        # Except (Assmpt. 5): ModuleNotFoundError
        except ModuleNotFoundError:
            # Install pyscog2
            !{sys.executable} -m pip install psycopg2-binary

            # Creating engine
            engine = create_engine(db_string)

        # Save the database into an SQL table:
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')

        # Print number of imported rows:
        rows_imported = 0

        # Get the start_time from time.time()
        start_time = time.time()
        for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)

            # Add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
        
# Except (Assmpt. 1):
# Print an instruction to import necessary modules
except:
    print('Import all necessary modules: json, pandas, numpy, re, create_engine from sqlalchemy, your password, sys, and time')

# Input variables into function
movies(f'{file_dir}wikipedia.movies.json',f'{file_dir}movies_metadata.csv',f'{file_dir}ratings.csv')