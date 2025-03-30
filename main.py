import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Trazendo os dados de conexão ao BD
load_dotenv()


def executar_query(query, params=None):
    conn = read_credentials()
    if not conn:
        return None

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
    except mysql.connector.Error as err:
        logging.error(f'Erro ao executar: {err}')
        return None
    finally:
        conn.close()


def read_credentials():
    # Configuração da Conexão
    config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    }
    try:
        # Tentativa da Conexão
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as err:
        logging.error(f'Erro ao conectar NO banco: {err}')
        return None


# Produtos mais caros
def top10():
    query = """
    SELECT PRODUCT_COD AS "Prod. Code",
           PRODUCT_NAME AS "Product Name",
           PRODUCT_VAL AS "Price (R$)"
    FROM data_product
    ORDER BY PRODUCT_VAL DESC
    LIMIT 10;
    """
    return executar_query(query)


# Seções de BEBIDAS e PADARIA
def secoes_departamentos():
    query = """
    SELECT DISTINCT SECTION_NAME
    FROM data_product
    WHERE DEP_NAME IN ('BEBIDAS', 'PADARIA');
    """
    return executar_query(query)


# Vendas no 1º trimestre de 2019
def vendas_por_area():
    query = """
    SELECT ds.BUSINESS_NAME,
           FORMAT(SUM(dss.SALES_VALUE), 2) AS Vendas_Totais
    FROM data_store_sales dss
    JOIN data_store_cad ds ON ds.STORE_CODE = dss.STORE_CODE
    WHERE dss.DATE BETWEEN '2019-01-01' AND '2019-03-31'
    GROUP BY ds.BUSINESS_NAME;
    """
    return executar_query(query)


# Função dinâmica retrieve_data
def retrieve_data(product_code, store_code, date_range):
    try:
        # Garantindo que as datas estão no formato correto

        start_date = datetime.strptime(
            date_range[0], "%Y-%m-%d").strftime("%Y-%m-%d")

        end_date = datetime.strptime(
            date_range[1], "%Y-%m-%d").strftime("%Y-%m-%d")

        query = """
        SELECT *
        FROM data_product_sales
        WHERE PRODUCT_CODE = %s
        AND STORE_CODE = %s
        AND DATE BETWEEN %s AND %s;
        """
        return executar_query(
            query, (int(product_code), int(store_code), start_date, end_date))

    except Exception as e:
        print(f"Erro ao processar as datas: {e}")
        return None


# Gráfico de venda
def plot_vendas():
    query = """
    SELECT STORE_CODE, SUM(SALES_VALUE) AS TOTAL_VENDAS
    FROM data_store_sales
    GROUP BY STORE_CODE
    ORDER BY STORE_CODE ASC;  # Alterado para manter a ordem numérica
    """
    df = executar_query(query)

    # Verificar estatísticas básicas
    print(df.describe())

    # Aplicar escala normal
    plt.figure(figsize=(14, 7))
    sns.set_style("darkgrid")

    ax = sns.barplot(
        x=df["STORE_CODE"],
        y=df["TOTAL_VENDAS"],
        color="#7D3C98",
        edgecolor="black",
        linewidth=3,
        width=0.5
    )

    # Configurações do gráfico
    plt.title("Total de Vendas por Loja", fontsize=20, fontweight="bold")
    plt.xlabel("Código da Loja", fontsize=16, fontweight="bold")
    plt.ylabel("Total de Vendas (R$)", fontsize=16, fontweight="bold")
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    ax.yaxis.set_label_coords(-0.07, 0.5)
    plt.grid(axis="y", linestyle="--", alpha=0.5)

    plt.show()


# Gráfico de avaliações IMDB
def plot_imdb():
    query = "SELECT rating FROM IMDB_movies;"
    df = executar_query(query)

    plt.figure(figsize=(12, 6))
    sns.set_style("darkgrid")

    ax = sns.histplot(
        df["rating"],
        bins=20,
        kde=True,
        color="#7D3C98",
        edgecolor="black",
        linewidth=2.0
    )

    plt.title(
        "Histograma das Avaliações de Filmes (IMDb)",
        fontsize=20, fontweight="bold")

    plt.xlabel("Notas (2-9)", fontsize=14)
    plt.ylabel("Número de Filmes por Faixa de Nota", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    ax.yaxis.set_label_coords(-0.07, 0.5)

    plt.show()


df_top_10 = top10()
print(df_top_10)

df_secoes = secoes_departamentos()
print(df_secoes)

df_vendas_area = vendas_por_area()
print(df_vendas_area)

df_vendas_prod = retrieve_data(18, 1, ['2019-01-01', '2019-01-31'])
print(df_vendas_prod)

plot_vendas()
plot_imdb()
