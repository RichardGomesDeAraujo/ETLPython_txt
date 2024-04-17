 <img src="HOD.png" align="Center" alt="Hands On Data" style="height: 180px; width:260px;"/>


# ETL with Python 
# Web Data, filtering, dropping columns, formatting and creating functions

## Data Source: https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/

## Data Type: .txt

## Code: [**ETLPython**](ETLPython.py#ETLPython.py)
<p>  <br>
  </p>

###### by [Richard Gomes de Araújo](https://github.com/RichardGomesDeAraujo) - 20/02/2024
[![Github Badge](https://img.shields.io/badge/-Github-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/RichardGomesDeAraujo)](https://github.com/RichardGomesDeAraujo)
[![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/richardaraujoanalistadedados/)](https://www.linkedin.com/in/richardaraujoanalistadedados/)
[![Youtube Badge](https://img.shields.io/badge/-YouTube-ff0000?style=flat-square&labelColor=ff0000&logo=youtube&logoColor=white&link=https://www.youtube.com/channel/UCc_jlqHut_GkXc8ahgQHOOw)](https://www.youtube.com/channel/UCc_jlqHut_GkXc8ahgQHOOw)
<p>  <br>
  </p>
  
# Index
- [**1**](README.md#1) - Importing Library

- [**2**](README.md#2) - Create Columns and Lines with a specific Data Layout

- [**3**](README.md#3) - Filtering Under Business Rule

- [**4**](README.md#4) - Dropping an Unuseful Column

- [**5**](README.md#5)- Formatting Date

- [**6**](README.md#6) - Formatting Number

- [**7**](README.md#7) - Visualizing the Data Types

- [**8**](README.md#8) - Counting Values (Grouping by a specific column)

- [**9**](README.md#9) - Finding Null Values and Counting Them

- [**10**](README.md#10) - Changing Null Values by Zero

- [**11**](README.md#11) - Processing the Object Data Type

- [**12**](README.md#12) - Calculating Memory Usage of Dataframe 1

- [**13**](README.md#13) - Analysing the Content of the Column with Object Data Type

- [**14**](README.md#14) - Transforming the Column with Object to Category Data Type

- [**15**](README.md#15) - Transforming the df into df2.parquet

- [**16**](README.md#16) - Calculating Memory Usage of Dataframe 2

- [**17**](README.md#17) - Creating Functions to Automate the Process

- [**18**](README.md#18) - Creating Function to Concat All the Files

- [**19**](README.md#19) - Creating a Function to Execute the Functions


---

### 1
Importing Library:
```python
# importing Library
import pandas as pd
import numpy as np
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 2
Create Columns and Lines with a specific Data Layout:
```python
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
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

 ### 3
Filtering Under Business Rule:
```python
# Filtering a column under a business rule
df = df[ df['CodBdi'] == 2.0]
df
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 4
Dropping an Unuseful Column:
```python
# Dropping an unuseful column where I need to use the command df.drop with number 1 to flag a column
df = df.drop(['CodBdi'], axis=1)
df
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 5
Formatting Date:
```python
# Formatting Date
df['DateDealing'] = pd.to_datetime( df['DateDealing'], format='%Y%m%d')
df
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 6
Formatting Number:
```python
# Formatting Numbers of the columns where I need to divide the number by 100 to obtain two decimals numbers
df['OpenPrice'] = (df['OpenPrice'] / 100).astype(float)
df['MaximumPrice'] = (df['MaximumPrice'] / 100).astype(float)
df['MinimumPrice'] = (df['MinimumPrice'] / 100).astype(float)
df['ClosePrice'] = (df['ClosePrice'] / 100).astype(float)
df['BusinessQuantity'] = df['BusinessQuantity'].astype(int)
df['BusinessVolume'] = df['BusinessVolume'].astype(int)
df
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 7
Visualizing the Data Types:
```python
df.dtypes

df.info()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 8
Counting Values (Grouping by a specific column):
```python
# Counting Values (Group by a specific column) Useful to identify outliers
df['ActionName'].value_counts()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 9
Finding Null Values and Counting Them:
```python
# Finding null values and counting them
df.isnull().sum()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 10
Changing Null Values by Zero:
```python
# Changing null values by zero where np.nan means null values in numpy library is igual nan and inplace=True to 
# transform the change permanent in the dataframe
df['BusinessVolume'].replace(np.nan, '0', inplace=True)
df
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 11
Processing the Object Data Type:
```python
# Processing the Object Data Type
# First: Looking the size of each column in the Dataframe
memory = df.memory_usage(deep=True)
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 12
Calculating Memory Usage of Dataframe 1:
```python
# Sum the total of memory my Dataframe is using dividing per 1024³ to discover the GB
memory.sum() / (1024 ** 3)
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 13
Analysing the Content of the Column with Object Data Type:
```python
# Counting how many differents datas we have in the object column grouping them
df['ActionSg'].value_counts()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 14
Transforming the Column with Object to Category Data Type:
```python
df['ActionSg'] = df['ActionSg'].astype('category')
df['ActionName'] = df['ActionName'].astype('category')
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 15
Transforming the df into df2.parquet:
```python
# Transforming File Type to .parquet
df.to_parquet('ETLWithPython.parquet')
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 16
Calculating Memory Usage of Dataframe 2:
```python
# Reading the .parquet file
df2 = pd.read_parquet('ETLWithPython.parquet')
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 17
Creating Functions to Automate the Process:
```python
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


```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 18
Creating Function to Concat All the Files:
```python
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
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

### 19
Creating a Function to Execute the Functions:
```python
# Function 6: Creating a function to execute the functions
yeardate = ['2022', '2023', '2024']
path = f'C:/Users/richa/Documents/ProjetosJupyter/'
namefile = 'COTAHIST_A'
# Obs.: I'm not using a dot after namefile or before typefile
typefile = 'txt'
finalfile = 'all_bovespa.csv'
concatfiles(path, namefile, yeardate, typefile, finalfile)
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

 
