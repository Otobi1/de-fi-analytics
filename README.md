**Project Overview**
analytics project for decentralised finance
![trad-fi analytics](de_fi_analytics.png)

[**Set up Infrastructure**](https://github.com/Otobi1/de-fi-analytics/blob/master/terraform/main.tf)
```
Provision and manage
- cloudbuild
- composer
- storage
- vm
- bigquery dataset
```

[**Cloudbuild Config**](https://github.com/Otobi1/de-fi-analytics/blob/master/cloudbuild.yaml)
```
- defined build steps including how to get the files from github into composer environment
```

**Ingest Data into GCS**

[**History**](https://github.com/Otobi1/de-fi-analytics/blob/master/scripts/ingest_history.py) and [**DAG**](https://github.com/Otobi1/de-fi-analytics/blob/master/dags/de_fi_history_coingecko_ingest.py)
> Ingest partitioned history data for the top 100 crypto coins into GCS. 

[**Hourly**](https://github.com/Otobi1/de-fi-analytics/blob/master/scripts/ingest_hourly.py) and [**DAG**](https://github.com/Otobi1/de-fi-analytics/blob/master/dags/de_fi_hourly_coingecko_ingest.py)
> Ingest partitioned and clustered hourly data for the top 100 coins into GCS

**Outcome**
> * Overcame the limitation from the trad-fi-analytics project by ensuring that the ingestion process and publishing raw data to Bigquery is automated using cloudbuild and composer.

**Limitation**
> * Composer quite expensive, exhausted the free credits very quickly even at lower config tier
> * Transformations not possible due to the exhaustion of the credits.