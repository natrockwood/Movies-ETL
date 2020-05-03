# Movies ETL

Starting this analysis, we used three datasets:
1. The wikipedia.movies.json file in this repository
2. The 'ratings' and 'movies_metadata' files found in Kaggle: https://www.kaggle.com/rounakbanik/the-movies-dataset/download (these files were too large to upload to the repo)

#### Checking competing data in both data sets.
We check the datasets to determine which data from each dataset we need. I put the results of our findings in the tables.

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

# Movies ETL Challenge
In this module's challenge, we streamlined the ETL process in a single code.
In doing that, there are a few things that can go wrong, and I've made assumptions for those below. In the code, I tried to get rid of those assumptions using ```try-except``` blocks.
The modules I imported were:
```
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import sys
import time
```
## Assumptions that may happen in the script run:
#### Assumption 1: User didn't import all the modules.
This was one of my initial errors, I didn't import all the modules I needed. Importing all the modules at the start of the notebook or code helps users know that they have what they need to continue on with the code.

The modules that I used in this code were imported as follows:
```
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import sys
import time
```

Importing these modules should be enough to run the code smoothly.

#### Assumption 2: Filepath created wasn't correct.
This was also another problem I encountered. Making sure the file path is correct ensures that the files will be pulled correctly and that files will actually be pulled. If files don't pull, then we won't have any data to work with.

#### Assumption 3: Datatypes can't be changed.
Datatypes are very crucial to make sure the code runs smoothly and correctly. Changing the datatypes to the ones that are needed ensures a smoothly running code. I changed some of the data types in the Kaggle Metadata to integers, numberic data, and datetime datatypes. Changing the datatypes also helps us if we need to use regular expressions.

#### Assumption 4: Columns didn't merge correctly.
Columns that don't merge correctly into one dataframe become difficult for other parts of the code to work. If we don't merge columns correctly, we might not be able to edit data types or replaces things that we need to replace.

#### Assumption 5: Engine module wasn't found.
Sometimes, not all modules we need were installed. In my case, I didn't have ```psycog``` installed. Many other people also ran into this problem. Catching this is essential, especially to people that haven't used this type of engine before, like me.
This was the code I used to install pyscog in my notebook:
```ModuleNotFoundError:
  # Install pyscog
  !{sys.executable} -m pip install psycopg2-binary```
