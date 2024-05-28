import pandas as pd
import logging

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_prepare_data(file_path):
    logging.info(f'Loading File: {file_path}')
    df = pd.read_csv(file_path)
    
    logging.info('Converting date column to datetime and changing the date format')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')
    
    logging.info('Filling nulls')
    df.fillna(0, inplace=True)
    
    return df

def enrich_data(df):
    logging.info('Creating Daily Variation')
    df['Daily Variation'] = df['Close'] - df['Open']
    df['Daily Variation'] = df['Daily Variation'].round(2).astype(str) + 'k'
    
    logging.info('Calculating the daily variation in %')
    df['Daily Variation (%)'] = df['Close'].pct_change() * 100
    df['Daily Variation (%)'] = df['Daily Variation (%)'].round(2)
    
    # Filling the NaN values with 0
    df['Daily Variation (%)'].fillna(0, inplace=True)
    
    # Converting to string and adding the '%'
    df['Daily Variation (%)'] = df['Daily Variation (%)'].astype(str) + ' %'
    
    logging.info('Calculating how many BTC were traded in that day')
    df['Volume Variation'] = df['Volume'].diff().fillna(0).astype('int64')
    
    logging.info('Creating the ID column')
    df["id"] = df.index + 1
    
    logging.info('Moving the ID column to the first column')
    cols = list(df.columns)
    cols.insert(0, cols.pop(cols.index('id')))
    df = df[cols]
    
    logging.info('Setting the datetime again')
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
    
    return df

def save_by_year(df):
    for year in range(2014, 2025):
        df_year = df[df['Date'].dt.year == year]
        logging.info(f'Year {year}: {len(df_year)} rows')
        
        file_name = f'BTC-HISTORY-{year}.csv'
        logging.info(f'Saving data from year {year} in {file_name}')
        df_year.to_csv(file_name, index=False)

def create_consolidated(df):
    logging.info('Creating a consolidated file')
    df_consolidated = df.copy()
    
    file_name = 'BTC-HISTORY-Consolidated.csv'
    logging.info(f'Saving the consolidated file in {file_name}')
    df_consolidated.to_csv(file_name, index=False)

def main():
    logging.info('Initiating the process')
    
    file_path = 'BTC-USD.csv'
    df = load_and_prepare_data(file_path)
    
    df = enrich_data(df)
    
    save_by_year(df)
    
    create_consolidated(df)
    
    logging.info('Success!')
    logging.info(df.head())
    logging.info(df.info())
    logging.info(df.describe())

if __name__ == "__main__":
    main()