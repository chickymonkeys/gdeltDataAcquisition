# requirements: numpy, scipy, pandas, bs4, requests, reverse_geocoder, dask
# python gdeltExtractDask.py 'data_path' 'output_name' 'cameo_codes' 'countries'
import pandas as pd
import numpy as np
import requests
import re
import sys
import multiprocessing
import reverse_geocoder as geo
import dask.dataframe as dd
import os

from dask.delayed import delayed
from dask import compute
from dask.distributed import Client, progress
from bs4 import BeautifulSoup as bs
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

"""Parse inline arguments from bash input

Parameters
----------
s : str
    a commma-separated string of codes

Returns
-------
list
    a list of codes as arguments
"""


def parseArgs(s):
    args = list(s.replace(' ', '').split(','))
    return args


"""Get the country code from a tuple of coordinates

Parameters
----------
coords : tuple
    a tuple of coordinates

Returns
-------
str
    the two-letters country code or np.nan where not found
"""


def geoCountry(coords):
    return geo.get(coords, mode=1).get('cc', np.nan)


"""Get the filenames from the index page of the GDELT server

Parameters
----------
mainpage : str
    the URL of the index page of the GDELT server

Returns
-------
list
    the list of files in the index page
"""


def scraperFiles(mainpage):
    # retrieve index_page content
    index = requests.get(mainpage)
    # assign BeautifulSoup object on response content
    soup = bs(index.content, 'html.parser')
    # get all hyperlinks to the .zip files
    hrefs = [a.get('href') for a in soup.find_all('a', {'href': True})]
    # generate list of files in the server to iterate after filtering
    redate = re.compile(r'^([0-9]{1,}).{1,}$')
    hrefs = [i for i in hrefs if redate.match(i)]
    # return list of URLs in the landing page
    return hrefs


"""Clean a subset of the GDELT project database

Parameters
----------
crumb : DataFrame
    a (daily) subset of the GDELT project database

Returns
-------
DataFrame
    the tidied subest of the GDELT project database
"""


def tidyCrumbs(crumb):

    # columns formatting other than string
    int_cols = ['GLOBALEVENTID', 'QuadClass', 'NumMentions',
                'NumSources', 'NumArticles', 'Actor1Geo_Type', 'Actor2Geo_Type']
    float_cols = ['AvgTone', 'Actor1Geo_Lat', 'Actor2Geo_Lat', 'Actor1Geo_Long',
                  'Actor2Geo_Long', 'ActionGeo_Lat', 'ActionGeo_Long']

    # leave just SQLDATE field in yyyymmdd format as string, drop all other
    # date definitions, coded information for the actions, Goldstein Scale
    # that we will obtain from CAMEO codes if needed
    crumb = crumb.drop(columns=['MonthYear', 'Year',
                                'FractionDate', 'ActionGeo_FeatureID', 'GoldsteinScale'])

    # change the root event dummy to byte
    # match numeric values all as float to avoid parsing errors
    crumb['IsRootEvent'] = crumb['IsRootEvent'].astype('byte')
    # match numeric values all as float to avoid parsing errors
    crumb[int_cols + float_cols] = crumb[int_cols + float_cols].replace(
        r'[^0-9.+-e]', '', regex=True).astype('float64')
    # remove events with no geocoding of action after formatting
    crumb = crumb.dropna(subset=['ActionGeo_Lat', 'ActionGeo_Long'])
    crumb = crumb[(crumb['ActionGeo_Lat'] != 0.0) &
                  (crumb['ActionGeo_Long'] != 0.0)]
    return crumb


"""Filter a subset of the GDELT database with the CAMEO codes of interest

Parameters
----------
crumb : DataFrame
    a (daily) subset of the GDELT project database
codes : list
    a list of CAMEO codes of interest at any level of detail

Returns
-------
DataFrame
    the filtered subset by CAMEO codes of interest
"""


def cameoFiltering(crumb, codes):
    return crumb[crumb['EventCode'].str.contains(
        '|'.join(map(str, codes)), na=False)]


"""Filter a subset of the GDELT database with the countries of interest

Parameters
----------
crumb : DataFrame
    a (daily) subset of the GDELT project database
codes : list
    a list of FIPS10-4 two-characters codes of countries of interest

Returns
-------
DataFrame
    the filtered subset by countries of interest
"""


def ccFiltering(crumb, codes):
    # boolean mask for missing country codes of action
    mask = crumb['ActionGeo_CountryCode'].isna()
    # list of tuples from coordinates in the data crumb
    cc = list(zip(crumb['ActionGeo_Lat'], crumb['ActionGeo_Long']))
    # perform reverse geocoding for true values of the boolean mask
    cc = [geoCountry(x) for x, y in zip(cc, mask) if y]
    # substitute missing country codes with the retrieved ones
    crumb.loc[crumb['ActionGeo_CountryCode'].isna(),
              'ActionGeo_CountryCode'] = cc
    # filtering by countries of interest
    return crumb[crumb['ActionGeo_CountryCode'].isin(codes)]


"""Data acquisition of a subset of the GDELT project database v1.0 that includes
    tidy and filtering operations for any datapoint given type of actions of
    interest and country locations.

Parameters
----------
out : str
    a string with the output path for the snippet of data after handling
file: str
    a string with the subset file name in the server
columns: list
    the header for the data extracted from the GDELT server
cameo: list
    a list of CAMEO codes of interest for filtering
ccs: list
    a list of FIPS10-4 country codes of interest for filtering
Returns
-------
DataFrame
    the acquired GDELT project data after exporting in .csv
"""


def extractCrumbs(out, file, columns, cameo, ccs):
    response = urlopen('http://data.gdeltproject.org/events/' + file)
    file_date = file.split('.')[0]

    # save .zip file in memory to spare I/O processing and space
    with BytesIO(response.read()) as tempfile:
        zipdata = ZipFile(tempfile)

        if int(file_date) < 201304:
            extension = '.csv'
        else:
            extension = '.export.CSV'

        """ read_csv format:
        comes as .zip file, tab separated and with no header """
        crumb = pd.read_csv(
            zipdata.open(file_date + extension),
            sep='\t',
            header=None,
            dtype=str,
            names=columns,
            index_col=False)

    if int(file_date) < 201304:
        crumb['SOURCEURL'] = np.nan

    crumb = tidyCrumbs(crumb)

    crumb = cameoFiltering(crumb, cameo)

    crumb = ccFiltering(crumb, ccs)

    crumb.to_csv(out + file_date + '.csv', sep=',',
                 index=False, header=True, na_rep='.')

    return crumb


if __name__ == '__main__':

    # Retrieve Inline Arguments
    data_path = sys.argv[1]
    filename = sys.argv[2]
    cameos = parseArgs(sys.argv[3])
    countries = parseArgs(sys.argv[4])

    # if data_path or temp do not exist, then create them
    if not os.path.isdir(data_path + 'temp'):
        if not os.path.isdir(data_path):
            print("Creation of the directory %s ..." % data_path)
            os.makedirs(data_path)
        print("Creation of the directory %s" % data_path + 'temp')
        os.makedirs(data_path + 'temp')

    # Dask Setup
    # n_cores = multiprocessing.cpu_count() - 1
    # client = Client(n_workers=n_cores, threads_per_worker=1)

    # Column Headers
    # entries before 2013 do not have the SOURCEURL field
    columns = pd.read_csv(
        'https://www.gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt',
        sep='\t', header=None).values.flatten().tolist()

    # GDELT v1.0 Daily Files Index Page
    hrefs = scraperFiles('http://data.gdeltproject.org/events/index.html')

    # Script Testing
    hrefs = hrefs[-112:-110]

    # compute([delayed(extractCrumbs)(
    #     DATA_PATH,
    #     href,
    #     columns,
    #     cameo_codes,
    #     countries_of_interest) for href in hrefs])

    for href in hrefs:
        print("Extracting " + href.split('.')[0] + " Events ... ", end=" ")
        # Acquire, Tidy, Filter GDELT
        ghost = extractCrumbs(data_path + 'temp/', href,
                              columns, cameos, countries)
        print("Done!")

    # concatenate crumbs of dataframe
    # gdeltData = pd.concat(crumbs)

    # export .csv data, missing values Stata style
    # gdeltData.to_csv(OUT_PATH + FILENAME, sep=',',
    #                  index=False, header=True, na_rep='.')

    # Aggregate all partitions and create unique .csv file
    df = dd.read_csv(data_path + 'temp/' + "*.csv").compute()
    df.to_csv(data_path + filename, index=False)
    print("New data in %s!" % data_path)

    # Remove all partitions and
    for f in os.listdir(data_path + 'temp/'):
        os.remove(os.path.join(data_path + 'temp/', f))
    os.rmdir(data_path + 'temp/')
    print("temp removed!")

    # client.shutdown()
