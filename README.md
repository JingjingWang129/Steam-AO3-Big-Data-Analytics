# Steam-AO3-Big-Data-Analytics

## Directory Navigation
* [Data Ingestion / HW6](./data_ingest/)
* [ETL Code / HW6,HW7](./etl_code/)
* [Profiling Code / HW6](./profiling_code/)
* [Analytics Code / HW8](./ana_code/)
* [Screenshots](./screenshots/)

## Operation Guide
* Run [worm.py](./data_ingest/worm.py) to retrieve the data about the most popular (most downloaded) 10000 games in steam using the steamspy api. Upload the dataset as "hdfs://nyu-dataproc-m/user/jw8191_nyu_edu/steamspy_top10000_raw.jsonl"
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
* Note: Some games are free-to-play, which means their price on steam is 0 and cannot be used to calculate the estimated revenue. We do not cover the analysis about free-to-play games in the project because we are not able for find their revenue using the existing datasets, if there's a further exploration, we might retrieve the revenue data from the financial statements of the developers of free-to-play games.
