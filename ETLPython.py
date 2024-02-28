# importing Libraries
import pandas as pd
import numpy as np

# Creating Columns and Lines
columnlayout = [(2,10),
                (10,12),
                (12,24),
                (27,39),
                (56,69),
                (69,82),
                (82,95),
                (108,121),
                (152,170),
                (170,188)
               ]

columnnames = ['DateDealing','CodBdi','ActionSg','ActionName','OpenPrice','MaximumPrice','MinimumPrice','ClosePrice','BusinessQuantity','BusinessVolume']

df = pd.read_fwf('C:/Users/richa/Documents/ProjetosJupyter/COTAHIST_A2022.txt', colspecs = columnlayout, names = columnnames, skiprows=1)
                 
df   


# Filtering a column under a business rule
df = df[ df['CodBdi'] == 2.0]
df


# Dropping an unuseful column where I need to use the command df.drop with number 1 to flag a column
df = df.drop(['CodBdi'], axis=1)
df


# Formatting Date
df['DateDealing'] = pd.to_datetime( df['DateDealing'], format='%Y%m%d')
df


# Formatting Numbers of the columns where I need to divide the number by 100 to obtain two decimals numbers
df['OpenPrice'] = (df['OpenPrice'] / 100).astype(float)
df['MaximumPrice'] = (df['MaximumPrice'] / 100).astype(float)
df['MinimumPrice'] = (df['MinimumPrice'] / 100).astype(float)
df['ClosePrice'] = (df['ClosePrice'] / 100).astype(float)
df['BusinessQuantity'] = df['BusinessQuantity'].astype(int)
df['BusinessVolume'] = df['BusinessVolume'].astype(int)
df


df.dtypes

df.info()


# Counting Values (Group by a specific column) Useful to identify outliers
df['ActionName'].value_counts()



# Finding null values and counting them
df.isnull().sum()


# Changing null values by zero where np.nan means null values in numpy library is igual nan and inplace=True to 
# transform the change permanent in the dataframe
df['BusinessVolume'].replace(np.nan, '0', inplace=True)
df


# Processing the Object Data Type
# First: Looking the size of each column in the Dataframe
memory = df.memory_usage(deep=True)


# Sum the total of memory my Dataframe is using dividing per 1024³ to discover the GB
memory.sum() / (1024 ** 3)


# Counting how many differents datas we have in the object column grouping them
df['ActionSg'].value_counts()


# Transforming the Column with Object to Category Data Type
df['ActionSg'] = df['ActionSg'].astype('category')
df['ActionName'] = df['ActionName'].astype('category')


df.info()


# Transforming File Type to .parquet
df.to_parquet('ETLWithPython.parquet')


# Reading the .parquet file
df2 = pd.read_parquet('ETLWithPython.parquet')



# The .parquet file maintain the data type category transformed before
df2.info()


# Calculanting the size of the .parquet file
memory2 = df2.memory_usage(deep=True)
memory2.sum() / (1024 ** 3)


# Creating Functions to Automate the Process
# Function 1
# Creating Columns and Lines with the layout and replacing the address file in the pd.read

def readfiles(path, namefile, yeardate, typefile):
    _file = f'{path}{namefile}{yeardate}.{typefile}'

    columnlayout = [(2,10),
                    (10,12),
                    (12,24),
                    (27,39),
                    (56,69),
                    (69,82),
                    (82,95),
                    (108,121),
                    (152,170),
                    (170,188)
                   ]

    columnnames = ['DateDealing','CodBdi','ActionSg','ActionName','OpenPrice','MaximumPrice','MinimumPrice','ClosePrice','BusinessQuantity','BusinessVolume']

    # replacing the address by the function _file
    df3 = pd.read_fwf(_file, colspecs = columnlayout, names = columnnames, skiprows=1)
    
    return df3


# Function 2
def filterstocks(df3):
    # Filtering a column under a business rule
    df3 = df3[ df3['CodBdi'] == 2.0]
    
    # Dropping an unuseful column where I need to use the command df.drop with number 1 to flag a column
    df3 = df3.drop(['CodBdi'], axis=1)
    
    return df3


# Function 3
def parsedate(df3):
    # Formatting Date
    df3['DateDealing'] = pd.to_datetime( df3['DateDealing'], format='%Y%m%d')

    return df3


# Function 4
def parsevalues(df3):
    # Formatting Numbers of the columns where I need to divide the number by 100 to obtain two decimals numbers
    df3['OpenPrice'] = (df3['OpenPrice'] / 100).astype(float)
    df3['MaximumPrice'] = (df3['MaximumPrice'] / 100).astype(float)
    df3['MinimumPrice'] = (df3['MinimumPrice'] / 100).astype(float)
    df3['ClosePrice'] = (df3['ClosePrice'] / 100).astype(float)
    df3['BusinessQuantity'] = df3['BusinessQuantity'].astype(int)
    df3['BusinessVolume'] = df3['BusinessVolume'].astype(int)
    
    return df3


# Function 5: Concat all the files
def concatfiles(path, namefile, yeardate, typefile, finalfile):
    # using enumerate to index the i value
    for i, y in enumerate(yeardate):
        df3 = readfiles(path, namefile, y, typefile)
        df3 = filterstocks(df3)
        df3 = parsedate(df3)
        df3 = parsevalues(df3)
        
        if i==0:
            dffinal = df3
        else:
            dffinal = pd.concat([dffinal, df3])
            
    # create a csv file with the result
    dffinal.to_csv(f'{path}//{finalfile}', index=False)
    


# Function 6: Creating a function to execute the functions
yeardate = ['2022', '2023', '2024']
path = f'C:/Users/richa/Documents/ProjetosJupyter/'
namefile = 'COTAHIST_A'
# Obs.: I'm not using a dot after namefile or before typefile
typefile = 'txt'
finalfile = 'all_bovespa.csv'
concatfiles(path, namefile, yeardate, typefile, finalfile)
    