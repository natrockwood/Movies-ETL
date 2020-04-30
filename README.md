# Movies-ETL

Starting this analysis, we used two datasets:
1. The wikipedia.movies.json file in this repository
2. The 'ratings' and 'movies_metadata' files found in Kaggle: https://www.kaggle.com/rounakbanik/the-movies-dataset/download (these files were too large to upload to the repo)

#### Checking competing data in both data sets.
Wiki | Movielens | Resolution
----------------------|------------------------|----------------------------
title_wiki | title_kaggle | **Drop Wikipedia**
running_time | runtime | **Keep Kaggle; fill in zeros with Wikipedia data.**
budget_wiki  | budget_kaggle | **Keep Kaggle; fill in zeros with Wikipedia data.**
box_office | revenue | **Keep Kaggle; fill in zeros with Wikipedia data.**
release_date_wiki | release_date_kaggle | **Drop Wikipedia data**
Language | original_language  | **Drop Wikipedia data**
Production company(s) | production_companies  | **Drop Wikipedia data**
- We dropped the wikipedia **title data** because the kaggle data was more accurate
- Since the Wikipedia data had a few outliers in the **runtime data**, we use the Kaggle data.
- Since the Wikipedia data had a few outliers in the **budget data**, we use the Kaggle data.
- Since the Wikipedia data had a few outliers in the **box office data**, we use the Kaggle data.
- There were some null values in the **release date** columns from the Wikipedia data, but not in Kaggle, so we just dropped it.
- Wikipedia data had more info on the different **languages** uses in the movies, however it's a little tricky to parse out each language. Since Kaggle data was already clean, and had what we needed, we just dropped Wikipedia data.
- Using this code ```movies_df[['Production company(s)','production_companies']]``` we can already see that Kaggle data is more consistent that Wiki data, hence, dropping wiki.
