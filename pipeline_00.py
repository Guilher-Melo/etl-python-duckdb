import os
from datetime import datetime

import duckdb
import gdown
import pandas as pd
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame
from sqlalchemy import create_engine

load_dotenv()


def conectar_banco():
    # Cria ou conecta banco duckdb
    return duckdb.connect(database='duckdb.db', read_only=False)


def inicializa_tabela(con):
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS historico_arquivos (
        nome_arquivo VARCHAR,
        horario_processamento TIMESTAMP
    )
    """
    )


def registrar_arquivo(con, nome_arquivo):
    # Registra um novo arquivo com horário atual
    con.execute(
        """
    INSERT INTO historico_arquivos(nome_arquivo, horario_processamento)
    VALUES (?, ?)
""", (nome_arquivo, datetime.now())
    )


def arquivos_processados(con):
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM\
                                             historico_arquivos").fetchall())


def baixar_arquivos_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local,
                          quiet=False, use_cookies=False)


def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv


def ler_csv(caminho_do_arquivo):
    return duckdb.read_csv(caminho_do_arquivo)


def transformar(df: DuckDBPyRelation) -> DataFrame:
    # Adiciona coluna transformando para dataframe pandas
    df_transformado = duckdb.sql(
        "SELECT *, quantidade * valor as total_vendas from df").df()
    return df_transformado


def salvar_no_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv('DATABASE_URL')
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    URL_DRIVE = os.getenv('URL_DRIVE')
    url_pasta = URL_DRIVE
    diretorio_local = './pasta_gdown'

    # baixar_arquivos_google_drive(url_pasta, diretorio_local)
    arquivos = listar_arquivos_csv(diretorio_local)
    con = conectar_banco()
    inicializa_tabela(con)
    processados = arquivos_processados(con)
    for arquivo in arquivos:
        nome_arquivo = os.path.basename(arquivo)
        if nome_arquivo not in processados:
            df_duck_db = ler_csv(arquivo)
            pandas_df_transformado = transformar(df_duck_db)
            salvar_no_postgres(pandas_df_transformado, "vendas_calculado")
            registrar_arquivo(con, nome_arquivo)
            print(f'Arquivo {nome_arquivo} processado e salvo.')
        else:
            print(f'Arquivo {nome_arquivo} já foi processado anteriormente')
