# GDELT Data Acquisition

A simple Python script to acquire data from the [GDELT Project Event Database](https://www.gdeltproject.org), one of the largest open datasets for understanding global human society, totaling more than 8.1 trillion datapoints spanning 200 years in 152 languages.

The Event Database contains over a quarter-billion records organized into a set of tab-delimited files by data. Through March 31, 2013 records are stored in monthly and yearly files by the date the event took place. Beginning with April 1, 2013, files are created daily and records are stored by the date the event was found in the world's news media rather than the date it occurred.

Our focus is on the version 1.0 of this database, which is updated daily with a new entry in the [Raw Data Files](http://data.gdeltproject.org/events/index.html).

## Description

This script extracts a dataset of events from the GDELT Project Event Database v1.0 Raw Data filtering by desired types of events using the CAMEO taxonomy and desired countries of action using FIPS 10-4 Country Codes. The given output is a comma-separated values file containing the identified events given a set of events and countries.

## Dependencies

This script runs with Python 3.9.x and requires the following packages: numpy, scipy, pandas, reverse_geocoder, requests and dask, which are available with `pip install`.

## Script Execution

You can run the main script `gdeltExtractDask.py` from command line using the following settings:

```bash
$ python gdeltExtractDask.py 'data_path' 'file_name' 'cameo_codes' 'countries'
```

where:

* `data_path` is a string that describes the path where you want your data to be saved;

* `file_name` is a string with the desired name for the final `.csv` file containing the results of your query;

* `cameo_codes` is a comma-separated list of [CAMEO codes](http://eventdata.parusanalytics.com/cameo.dir/CAMEO.Manual.1.1b3.pdf) that indicate the type of events you are eager to extract;

* `countries` is a comma-separated list of [FIPS 10-4 Country Codes](https://en.wikipedia.org/wiki/List_of_FIPS_country_codes) of countries where the previously indicated type of events happened.

## About

Alessandro Pizzigolotto (NHH) | [@chickymonkeys](https://twitter.com/chickymonkeys) | [Personal Website](https://pizzigolot.to)