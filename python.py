import pandas as pd
import numpy as np
from pandas import json_normalize
from pymongo import MongoClient
from urllib.parse import quote
from sklearn.model_selection import train_test_split
import psycopg2 as pg
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

## Stage 1: Importing Data
### Complaints
df=pd.read_csv('complaints-2.csv')

### Demographics
# Replace 'your_connection_string_here' with your actual MongoDB Atlas connection string
# connection_string = "mongodb+srv://database_project:@cluster0.rfpiq03.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)

# Replace 'your_database' and 'your_collection' with your actual database and collection names
db = client['your_database']
collection = db['your_collection']

from pandas import json_normalize

#example template to show how fields are selected
projection_template = {
    "state": 1,
    "county":1,
    "_id": 0,
}
fields = [
    "unemployment.employed",
    "unemployment.unemployed",
    "population_by_age.total.18_over",
    "population_by_age.total.65_over",
]
#years for which we want the data
years = range(2011, 2020)
formatted_fields = []

#append the required fields along with the years to get the final names
for field in fields:
    for year in years:
        formatted_fields.append(f"{field}.{year}")

# Append the formatted fields to the projection_template
for formatted_field in formatted_fields:
    projection_template[formatted_field] = 1

# Perform the query with the specified projection
cursor = collection.find({}, projection_template)

# Convert the cursor to a list of dictionaries
queried_data = list(cursor)

#Converting data into a dataframe
df_demo = pd.DataFrame(queried_data)
flat_df = json_normalize(df_demo.to_dict(orient='records'))

#rough copy so that incase of an error, we do not have to import the entire dataset again
df_demo=flat_df.copy()

## Stage 2: Data Transformation
### Complaints
def eda_complaints(df):
    #removing unneccesary columns
    df = df.drop(columns=['Tags','ZIP code'])
    #converting date to datetime
    df['Date received'] = pd.to_datetime(df['Date received'])

    # Specify columns for which you want to replace NaN values with random values
    columns_to_replace = ['State', 'Consumer disputed?','Company response to consumer',
                         'Timely response?','Submitted via']

    # Replace NaN values with random values from the same column
    for column in columns_to_replace:
        nan_mask = df[column].isna()
        num_nan_values = nan_mask.sum()
        if num_nan_values > 0:
            # Generate random values within the range of non-NaN values in the column
            random_values = np.random.choice(df[column].dropna(), num_nan_values)
            # Assign the random values to NaN positions in the column
            df.loc[nan_mask, column] = random_values
    
    # Add condition to filter rows based on the 'year' column
    df = df[(df['Date received'] >= '2010-01-01') & (df['Date received'] <= '2020-12-31')]
    df['Date received']=pd.to_datetime(df['Date received'])
    
    # Extract unique combinations of 'product', 'sub-product', 'issue', and 'sub-issue'
    unique_combinations = df[['Product', 'Sub-product', 'Issue', 'Sub-issue']].drop_duplicates()

    # Iterate over the rows with NaN values and fill with unique combinations
    for index, row in df[df.isna().any(axis=1)].iterrows():
        nan_columns = row.index[row.isna()]
        for column in nan_columns:
            if column in unique_combinations.columns:
                unique_values = unique_combinations[column].values
                if len(unique_values) > 0:
                    df.at[index, column] = np.random.choice(unique_values)
    
    #Converting binary columns to numeric
    columns_to_map = ['Timely response?', 'Consumer disputed?']

    for column in columns_to_map:
        df[column] = df[column].map({'Yes': 1, 'No': 0})


    return df
#run the function above
df_eda=eda_complaints(df)

### Demographics
#function for eda
def eda_demo(df_demo):
    #removing columns
#     df_demo = df_demo.drop(columns=['county_division_name','county_region_name','state_name','zipcode','lng','lat','county','major_city','zipcode_type','timezone','post_office_city',
#                                    'radius_in_miles','water_area_in_sqmi','county_fips','state_fips',
#                                    'county_sumlev','county_region','county_division','land_area_in_sqmi'],axis=1)
    # Removing 'census' from column names
    filtered_columns = [col for col in df_demo.columns if 'census' not in col.lower()]

    # Create a new DataFrame with the filtered columns
    df_demo = df_demo[filtered_columns]

    # Assuming 'temp' is your original DataFrame

    # Melt the DataFrame
    melted_df = pd.melt(df_demo, id_vars=['state','county'], var_name='column_name', value_name='value')

    # Extract the year from 'column_name' and create a new column 'year'
    melted_df[['category', 'year']] = melted_df['column_name'].str.rsplit('.', n=1, expand=True)
    # Drop the original column
    melted_df = melted_df.drop(columns='column_name')

    # Replace values in the 'category' column
    melted_df['category'] = melted_df['category'].replace({
        'unemployment.employed': 'population.unemployment.employed',
        'unemployment.unemployed': 'population.unemployment.unemployed'
    })

    # Rename the value column to 'population'
    melted_df = melted_df.rename(columns={'value': 'population'})

    # Remove the word 'population' and 'by_' from each category in the 'category' column
    melted_df['category'] = melted_df['category'].str.replace('population.', '', regex=True).str.replace('by_', '', regex=True)

    # Sort the DataFrame by the index columns
    melted_df.sort_values(['state','county', 'year'], inplace=True)

    # Filter rows where the 'year' column is greater than or equal to 2010
    melted_df['year']=melted_df['year'].astype(int)
#     melted_df = melted_df[(melted_df['year'] >= 2011) and melted_df['year']<2020]
    
    # Apply forward fill separately for each group
#     filled_df = melted_df.groupby(['major_city', 'county', 'state', 'lat', 'lng', 'land_area_in_sqmi', 'zipcode', 'state_name', 'county_region_name', 'county_division_name', 'year']).ffill()

    # Pivot the DataFrame
    pivoted_df = melted_df.pivot_table(index=['state', 'county','year'],
                                       columns='category', values='population', aggfunc='first')

    # Reset the index to make 'year' a separate column
    pivoted_df.reset_index(inplace=True)
    
    # Identify numeric and string columns
    numeric_columns = pivoted_df.select_dtypes(include=['number']).columns
    string_columns = pivoted_df.select_dtypes(include=['object']).columns

    # Fill NaN values in numeric columns with the median
    for column in numeric_columns:
        pivoted_df[column].fillna(
            pivoted_df.groupby('county')[column].transform('median'),
            inplace=True
        )

    # Fill NaN values in string columns with the most common value (mode)
    for column in string_columns:
        pivoted_df[column].fillna(
            pivoted_df.groupby('county')[column].transform(lambda x: x.mode().iloc[0]),
            inplace=True
        )
    #drop county
    pivoted_df = pivoted_df.drop(columns='county',axis=1)
    
    return pivoted_df

#run the function above
df_demo_eda=eda_demo(df_demo)

## Stage 3: Transferring Data

# PostgreSQL connection parameters
username = ''
password = ''
host = ''
port = ''
database = ''

# URL encode the password
encoded_password = quote(password)

# Create the connection string
DATABASE_URI = f'postgresql://{username}:{encoded_password}@{host}:{port}/{database}'

# Create an SQLAlchemy engine
engine = create_engine(DATABASE_URI, pool_size=10, max_overflow=20)

try:
    # Attempt to connect to the database
    connection = engine.connect()
    print("Connected to the database.")
    connection.close()
except OperationalError as e:
    print(f"Failed to connect to the database. Error: {e}")

# Use the Pandas to_sql function to create the table in the database
df_eda.to_sql('complaints', engine, index=False, if_exists='replace')
df_demo_eda.to_sql('demographics', engine, index=False, if_exists='replace')

# Print a message indicating success
print(f'Data has been transfered successfully.')
