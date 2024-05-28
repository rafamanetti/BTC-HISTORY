import pandas as pd
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_prepare_data(file_path):
    logging.info(f'Carregando dados do arquivo: {file_path}')
    df = pd.read_csv(file_path)
    
    logging.info('Convertendo coluna Date para datetime e mudando para o formato dia-mes-ano')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')
    
    logging.info('Preenchendo valores nulos')
    df.fillna(0, inplace=True)
    
    return df

def enrich_data(df):
    logging.info('Criando variação diária')
    df['Daily Variation'] = df['Close'] - df['Open']
    df['Daily Variation'] = df['Daily Variation'].round(2).astype(str) + 'k'
    
    logging.info('Calculando variação diária em percentual')
    df['Daily Variation (%)'] = df['Close'].pct_change() * 100
    df['Daily Variation (%)'] = df['Daily Variation (%)'].round(2)
    
    # Preencher os valores NaN com 0
    df['Daily Variation (%)'].fillna(0, inplace=True)
    
    # Converter para string e adicionar sufixo
    df['Daily Variation (%)'] = df['Daily Variation (%)'].astype(str) + ' %'
    
    logging.info('Calculando quantas BTC foram negociadas no dia')
    df['Volume Variation'] = df['Volume'].diff().fillna(0).astype('int64')
    
    logging.info('Criando coluna ID')
    df["id"] = df.index + 1
    
    logging.info('Movendo a coluna ID para a primeira posição')
    cols = list(df.columns)
    cols.insert(0, cols.pop(cols.index('id')))
    df = df[cols]
    
    logging.info('Adicionando novamente a conversão para datetime')
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
    
    return df

def save_by_year(df):
    for year in range(2014, 2025):
        df_year = df[df['Date'].dt.year == year]
        logging.info(f'Ano {year}: {len(df_year)} linhas')
        
        file_name = f'BTC-HISTORY-{year}.csv'
        logging.info(f'Salvando dados do ano {year} em {file_name}')
        df_year.to_csv(file_name, index=False)

def create_consolidated(df):
    logging.info('Criando DataFrame consolidado')
    df_consolidated = df.copy()
    
    file_name = 'BTC-HISTORY-Consolidated.csv'
    logging.info(f'Salvando DataFrame consolidado em {file_name}')
    df_consolidated.to_csv(file_name, index=False)

def main():
    logging.info('Iniciando o processo')
    
    file_path = 'BTC-USD.csv'
    df = load_and_prepare_data(file_path)
    
    df = enrich_data(df)
    
    save_by_year(df)
    
    create_consolidated(df)
    
    logging.info('Processo concluído')
    logging.info(df.head())
    logging.info(df.info())
    logging.info(df.describe())

if __name__ == "__main__":
    main()