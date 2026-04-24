# Read Me!

This is a personal data project to help expand my skills in data scraping, DBT modeling, and Tableau visualization. You can find Tableau visualizations on my Tableau Public profile or by following this [LINK](https://public.tableau.com/app/profile/parker.arnold/viz/USLChampionshipData/PlayerSeasonRatingsComp#1)! I have also written a growing number of posts for [RBLRSports.com](https://rblrsports.com/author/parnold/) leveraging the data from this work. If you enjoy the work I have done and want to contribute, leave me a tip on [Ko-Fi](https://ko-fi.com/parkera)!


# General Info & Getting Started

If you are interested in scraping data from FBREF.com as I have for this project, be sure to install all of the packages in the requirements.txt doc included in this repo. This will insure you have all of the necessary functions to scrape and then maniulate the data for storage.

Once you have the required packages, you can begin with the python scripts in the data_extraction folder. The files are designed to build off of each other, beginning first by defining the seasons you want to scrape in fbref_season_scraper.py. 

Then, you can scrape match results for each defined season using fbref_match_scraper.py which pulls all of the match results and high-level data for each season outlined in fbref_season_scraper.py. 

Once you have all of the match leve data, fbref_player_match_scraper.py is designed to look at individual player level data for each match to identify goal scoreres, starters, and other individual stats. There are some custom filters defined in this script thatv will allow you to specify individual seasons and match dates. These are here due to the number of pages that we need to scrape and the potential to run into timeouts during that process. It gives you the opportunity to pick back up where the script left off in the event of those errors.

For smaller, in-season updates the fbref_in_season_updater.py file is setup to scrape matches and player-level data for any matches that have completed since the last time you scraped the data. This file combines both match-level and player-level into a singular script due to the limited number of pages that are needed for in-season updated

After scraping, I created some models in dbt to transform the data into a more useable format and built some visualizations based on those models in Tableau. I plan to continue to manage/maintain this repo for upcming seasons and add visualizations along the way!

Thanks for checking out my work!
