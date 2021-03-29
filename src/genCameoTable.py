import pandas as pd
import os


def genCameoTable(csvName='cameo_table.csv', csvPath="."):
    """ this function returns a .csv file containing a match for
        CAMEO codes with description and Goldstein scale using an
        external source I found in the web 
        example : cameo = genCameoTable(csvPath = '../out') """

    # import .txt file as .csv : skip first two rows and ':' delimiter
    df = pd.read_csv(
        'http://eventdata.parusanalytics.com/cameo.dir/CAMEO.SCALE.txt',
        sep=':',
        header=None,
        dtype={'cameo_code': 'str'},
        names=['cameo_code', 'cameo_descr'],
        skiprows=2)

    # strip the Goldstein data scale to its own column
    df['cameo_goldstein'] = df['cameo_descr'].apply(
        lambda x: pd.Series(x.split(']')[0].strip('[]'),
                            name='goldsteinScale'))

    # strip only description text
    df['cameo_descr'] = df['cameo_descr'].apply(
        lambda x: pd.Series(x.split(']')[1].lstrip(' ').rstrip(' '),
                            name='cameoCodeDescription'))

    # strip whitespace from code and descr column
    df['cameo_code'] = df['cameo_code'].apply(lambda x: x.strip())
    # df['cameo_descr'] = df['cameo_descr'].apply(lambda x: x.strip())

    # set the index to the cameoCode for json lookups
    df.set_index(df.cameo_code.values, inplace=True)

    # rename the index to cameo_code
    df.index.rename('cameo_code', inplace=True)

    # export table to .csv
    pathname = os.path.join(csvPath, csvName)
    df.to_csv(pathname, sep=',', index=False)

    # return dataframe with codes
    return df
