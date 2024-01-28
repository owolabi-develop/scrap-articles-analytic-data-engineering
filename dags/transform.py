import glob
import pandas as pd
import os
def transform_data(extractpath,transformpath):
    csv_files = glob.glob(f'{extractpath}/*.csv')

    # Initialize an empty list to store DataFrames
    data_list = (pd.read_csv(file,sep=',',parse_dates=True).drop_duplicates().dropna() for file in csv_files)
    
    big_df = pd.concat(data_list,ignore_index=True)

    big_df['retrieval_date'] = pd.to_datetime(big_df['retrieval_date'])

    print(big_df.head())
    
    if not os.path.exists(transformpath):
        os.makedirs(transformpath)
    # Write the concatenated data to the 'tranformdata.csv' file
    big_df.to_json(f"{transformpath}/tranformdata.json",orient='records')

#transform_data('latestArticles/extract','latestArticles/transform')
    
