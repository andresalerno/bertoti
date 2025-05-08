import sqlite3
import os
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import time

# Carregar as variáveis do arquivo .env
load_dotenv()

# Carregar a URL do banco de dados a partir do .env
DATABASE_URL = os.getenv("DATABASE_URL")
caminho_banco = DATABASE_URL.split("sqlite:///")[1]

# Conectar ao banco de dados SQLite (abrir a conexão uma vez no início)
conn = sqlite3.connect(caminho_banco)
cursor = conn.cursor()

# Função para criar a tabela de balanços no banco de dados
def criar_tabela_balanco(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS empresas_balanco (
            Symbol TEXT,
            Periodo TEXT,
            Ativos_Circulantes REAL,
            Passivos_Circulantes REAL,
            DebtToEquity REAL,
            PRIMARY KEY (Symbol, Periodo)
        )
    """)
    cursor.connection.commit()
  # Salva a tabela no banco de dados

# Função para buscar os dados de balanço (Ativo Circulante e Passivo Circulante)
# Função para buscar dados do balanço
def buscar_dados_balanco(symbol):
    empresa = yf.Ticker(symbol)
    balanco = empresa.balance_sheet
    info = empresa.info  # Para pegar o DebtToEquity

    try:
        # Busca os ativos e passivos
        if 'Current Assets' in balanco.index and 'Current Liabilities' in balanco.index:
            Ativos_Circulantes = balanco.loc['Current Assets'].iloc[0]
            Passivos_Circulantes = balanco.loc['Current Liabilities'].iloc[0]

        else:
            print(f"Dados de 'Current Assets' ou 'Current Liabilities' não encontrados para o ticker {symbol}")
            return None

        # DebtToEquity
        DebtToEquity = info.get('debtToEquity', None)  # Usa .get() para evitar KeyError se faltar

        # Período (você pode melhorar para pegar o período exato da linha do balanço se quiser)
        Periodo = str(balanco.columns[0]) if len(balanco.columns) > 0 else "Desconhecido"

        return [(symbol, Periodo, Ativos_Circulantes, Passivos_Circulantes, DebtToEquity)]

    except Exception as e:
        print(f"Erro ao obter dados de balanço para o ticker {symbol}: {e}")
        return None



# Função para salvar os dados no banco de dados
def salvar_dados_balanco(cursor, dados):
    try:
        cursor.executemany("""
            INSERT OR IGNORE INTO empresas_balanco 
            (Symbol, Periodo, Ativos_Circulantes, Passivos_Circulantes, DebtToEquity)
            VALUES (?, ?, ?, ?, ?)
        """, dados)
        cursor.connection.commit()
        print(f"{len(dados)} registros de balanço inseridos.")
    except sqlite3.OperationalError as e:
        print(f"Erro ao inserir dados de balanço: {e}")


# Função para obter os tickers da tabela empresas e buscar os dados de balanço
# Função para obter os dados de balanço para várias empresas
def obter_dados_balanco(cursor):
    cursor.execute("SELECT Symbol FROM empresas")
    symbols = [row[0] for row in cursor.fetchall()]

    for i, symbol in enumerate(symbols, start=1):
        if symbol and symbol.strip() and '^' not in symbol and '/' not in symbol and ' ' not in symbol:
            print(f"[{i}/{len(symbols)}] Coletando dados para {symbol}...")
            try:
                dados_ticker = buscar_dados_balanco(symbol)
                if dados_ticker:
                    salvar_dados_balanco(cursor, dados_ticker)
                time.sleep(0.5)
            except Exception as e:
                print(f"Erro ao processar o ticker {symbol}: {e}")
        else:
            print(f"Símbolo inválido ou contendo caracteres especiais: {symbol}")



# Função para execução imediata
def executar_script():
    print("Iniciando a coleta de dados de balanço...")  
    try:
        criar_tabela_balanco(cursor)
        obter_dados_balanco(cursor)
        print("Dados de balanço coletados e salvos com sucesso!")  
    finally:
        conn.close()

# Rodar a função imediatamente para testar (sem agendamento)
executar_script()
