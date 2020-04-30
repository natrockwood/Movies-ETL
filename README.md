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
Language | original_language
Production company(s) | production_companies     
- We dropped the wikipedia data because the kaggle data was more accurate
- Since the Wikipedia data had a few outliers in the runtime data, we use the Kaggle data.
- Since the Wikipedia data had a few outliers in the budget data, we use the Kaggle data.
- Since the Wikipedia data had a few outliers in the box office data, we use the Kaggle data.
- There were some null values in the release date columns from the Wikipedia data, but not in Kaggle, so we just dropped it.
