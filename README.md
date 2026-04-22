# Steam-AO3-Big-Data-Analytics

## Directory Navigation
* [Data Ingestion / HW6](./data_ingest/)
* [ETL Code / HW6,HW7](./etl_code/)
* [Profiling Code / HW6](./profiling_code/)
* [Analytics Code / HW8](./ana_code/)
* [Screenshots](./screenshots/)

## Operation Guide
* Run [worm.py](./data_ingest/worm.py) to retrieve the data about the most popular (most downloaded) 10000 games in steam using the steamspy api (the data refreshed daily). Upload the dataset as "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_top10000_raw.jsonl"
* Run [CountRecs.scala](./profiling_code/CountRecs.scala) to have a general view about the dataset.
* Run [Clean.scala](./etl_code/Clean.scala) to remove the empty columns and create the analytical columns:  
  col("price_usd") = col("price")/100.0  
  col("owners_mid") = avg of the high and low range of col("owners")  
  col("rating") = col("positive")/(col("positive")+col("negative"))  
  col("estimated_revenue") = col("price_usd")*col("owners_mid")  
I then save the cleaned dataset as "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_cleaned"
* Run [FirstCode.scala](./profiling_code/FirstCode.scala) to explore the mean, mediam, std, min, max value and the mode of the numerical columns in the cleaned dataset, then create a binary column col("is_top_rated") for col("rating")>0.8 to explore the genral public reputation of the games in the dataset. (The uploaded code also include my teammate's part exploring her dataset.)
* Run [FinalCode.scala](./ana_code/FinalCode.scala) to find out the result. We created a new binary column col("is_high_female_engagement") based on (Number of Active Game Players / Number of AO3 Works per Game), then compare it against col("is_top_rated") to have col("rating_engagement_combo") that devides all games into four combinations:  
  11 (high steam rating + high female reputation)  
  10 (high steam rating + low female reputation)  
  01 (low steam rating + high female reputation)  
  00 (low steam rating + low female reputation)  
We then explore the distribution of estimated revenue and average word count of AO3 works of the games in different combinations to make further analysis.
Save the final dataset as "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/final_joined_analysis".
* Note: Some games are free-to-play, which means their price on steam is 0 and cannot be used to calculate the estimated revenue. We do not cover the analysis about free-to-play games in the project because we are not able for find their revenue using the existing datasets, if there's a further exploration, we might retrieve the revenue data from the financial statements of the developers of free-to-play games.

## Explanation of the Execution Result Screenshots
* Running result of CountRecs.scala: [pic1](screenshots/result_of_CountRecs_and_Clean/image001.png)
  [pic12](.screenshots/result_of_CountRecs_and_Clean/image002.png)
* Running result of Clean.scala: [pic3](.screenshots/result_of_CountRecs_and_Clean/image003.png)
  [pic4](.screenshots/result_of_CountRecs_and_Clean/image004.png)
  [pic5](.screenshots/result_of_CountRecs_and_Clean/image005.png)
  [pic6](.screenshots/result_of_CountRecs_and_Clean/image006.png)
* Check the cleaned dataset: [pic7](.screenshots/result_of_CountRecs_and_Clean/image007.png)
  [pic8](.screenshots/result_of_CountRecs_and_Clean/image008.png)
* Created the table in Hive: [pic9](.screenshots/result_of_CountRecs_and_Clean/image009.png)
  [pic10](.screenshots/result_of_CountRecs_and_Clean/image010.png)
  [pic11](.screenshots/result_of_CountRecs_and_Clean/image011.png)
  [pic12](.screenshots/result_of_CountRecs_and_Clean/image012.png)
* Result of FirstCode.scala ---- Exploring my Steamspy dataset:
  [pic1](.screenshots/result_of_FirstCode/image001.png)
  [pic2](.screenshots/result_of_FirstCode/image002.png)
  [pic3](.screenshots/result_of_FirstCode/image003.png)
  [pic4](.screenshots/result_of_FirstCode/image004.png)
  [pic5](.screenshots/result_of_FirstCode/image005.png)
  [pic6](.screenshots/result_of_FirstCode/image006.png)
  [pic7](.screenshots/result_of_FirstCode/image007.png)
  [pic8](.screenshots/result_of_FirstCode/image008.png)
* Result of FirstCode.scala ---- Exploring my teammate's AO3 dataset:
  [pic9](.screenshots/result_of_FirstCode/image009.png)
  [pic10](.screenshots/result_of_FirstCode/image010.png)
* Result of the FinalCode.scala ---- Processing Procedure:
  [pic1](.screenshots/result_of_FinalCode/image001.png)
  [pic2](.screenshots/result_of_FinalCode/image002.png)
  [pic3](.screenshots/result_of_FinalCode/image003.png)
* Result of the FinalCode.scala ---- Analysis Results:
  [pic4](.screenshots/result_of_FinalCode/image004.png)

## Database Authorization
I have provide access to pd2672_nyu_edu and adm209_nyu_edu.  
hdfs dfs -setfacl -m user:pd2672_nyu_edu:r-x /user/jw8191_nyu_edu
