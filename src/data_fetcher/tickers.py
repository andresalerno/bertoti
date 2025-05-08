import sqlite3
import os
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd

# Carregar as variáveis do arquivo .env
load_dotenv()

# Carregar a URL do banco de dados a partir do .env
DATABASE_URL = os.getenv("DATABASE_URL")
caminho_banco = DATABASE_URL.split("sqlite:///")[1]

# Carregar o CSV com o delimitador correto (supondo que seja ponto e vírgula)
file_path = './data/nasdaq_screener.csv'  # Substitua pelo caminho correto
data = pd.read_csv(file_path, delimiter=';')  # Usando ponto e vírgula como delimitador

# Verificar as primeiras linhas para garantir que as colunas estão corretas
print(data.head())

# Conectar ao banco de dados SQLite (abrir a conexão uma vez no início)
conn = sqlite3.connect(caminho_banco)
cursor = conn.cursor()

# Função para criar a tabela no banco de dados
def criar_tabela(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS empresas (
            Symbol TEXT PRIMARY KEY,
            Name TEXT,
            Last_Sale TEXT,
            Net_Change REAL,
            Percent_Change REAL,
            Market_Cap TEXT,
            Country TEXT,
            IPO_Year INTEGER,
            Volume INTEGER,
            Sector TEXT,
            Industry TEXT
        )
    """)
    cursor.connection.commit()  # Salva a tabela no banco de dados

# Função para salvar os dados no banco
def salvar_dados_no_banco(cursor, dados):
    dados_processados = []
    
    for _, row in dados.iterrows():
        # Verifique se as colunas estão corretas
        print(row)  # Imprimir a linha para verificar se as colunas estão corretas

        symbol = row['Symbol']
        name = row['Name']
        last_sale = row['Last Sale']
        net_change = row['Net Change']
        percent_change = row['% Change']
        market_cap = row['Market Cap']
        country = row['Country']
        ipo_year = row['IPO Year']
        volume = row['Volume']
        sector = row['Sector']
        industry = row['Industry']

        # Adicionando os dados na lista de inserção
        dados_processados.append((symbol, name, last_sale, net_change, percent_change, market_cap, country, ipo_year, volume, sector, industry))

    # Inserir os dados no banco de dados
    cursor.executemany("""
        INSERT OR IGNORE INTO empresas (Symbol, Name, Last_Sale, Net_Change, Percent_Change, Market_Cap, Country, IPO_Year, Volume, Sector, Industry)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, dados_processados)
    cursor.connection.commit()
    print(f"{len(dados_processados)} registros inseridos.")  # Log de quantidade de registros

# Criar a tabela
criar_tabela(cursor)

# Salvar os dados no banco de dados
salvar_dados_no_banco(cursor, data)

# Fechar a conexão após a execução
conn.close()
