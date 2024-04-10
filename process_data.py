import sys
import pandas as pd
from sqlalchemy import create_engine

def extract_data(messages_file_path, categories_file_path):
  # load datasets
  messages = pd.read_csv('messages_filepath')
  categories = pd.read_csv('categories_filepath')
  
  #merge datasets
  merged_df = pd.merge(messages, categories, on='id')
  return merged_df
  # create a dataframe of the 36 individual category columns
def transform_data(merged_df):
  categories_split = merged_df['categories'].str.split(';', expand=True)
  # select the first row of the categories dataframe
  row = categories_split.iloc[0:1]
  # use this row to extract a list of new column names for categories.
  # one way is to apply a lambda function that takes everything
  # up to the second to last character of each string with slicing
  category_colnames = row.apply(lambda x: x.str[:-2]).values.tolist()
  # rename the columns of `categories`
  categories_split.columns = category_colnames
  for column in categories_split:
    # set each value to be the last character of the string
    categories_split[column] = categories_split[column].str[-1]
    # convert column from string to numeric
    categories_split[column] = pd.to_numeric(categories_split[column])
    
  # drop the original categories column from `df`
  merged_df.drop(['categories'], axis=1, inplace=True)
  # concatenate the original dataframe with the new `categories` dataframe
  df_concat = pd.concat([merged_df, categories_split], join='inner', axis=1)
  # drop duplicates
  df_concat.drop_duplicates(inplace=True)
  return df_concat

def load_data(df_concat, database_filename):
  # save merged messages and categories data to SQLite database
  engine = create_engine('sqlite:///'+database_filename)
  df_concat.to_sql('DisasterResponse', engine, if_exists = 'replace', index=False)

def main():
    """Main function"""
    if len(sys.argv) == 4:
        # Extract arguments
        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        # Extract data
        print(f'Extracting data...\n    MESSAGES: {messages_filepath}\n    CATEGORIES: {categories_filepath}')
        messages, categories = load_data(messages_filepath, categories_filepath)

        # Transform data
        print('Transforming data...')
        df = transform_data(df)

        # Save data
        print(f'Loading data...\n    DATABASE: {database_filepath}')
        load_data(df, database_filepath)

        print('Wrangled data saved to database!')
    else:
        print('Please provide correct filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the wrangled data '\
              'to as the third argument. \n\nExample: python script_name.py '\
              'messages.csv categories.csv '\
              'DisasterResponse.db')

if __name__ == '__main__':
    main()
  