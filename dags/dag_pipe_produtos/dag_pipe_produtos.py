#region Imports

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import requests
import pandas as pd
import json
from dag_pipe_produtos import utils
from google.cloud import bigquery

# endregion

#region Inicio Variaveis Globais

env_var = utils.get_variable("config", deserialize_json=True)
variavel_api = env_var["caminho_api"]
variavel_acesso = env_var["GOOGLE_APPLICATION_CREDENTIALS"]
variavel_camadas_medalhao = env_var["camadas"]
variavel_camada_raw = env_var["camadas"][0]
variavel_camada_trusted = env_var["camadas"][1]
variavel_camada_refined = env_var["camadas"][2]
variavel_tabela_escrita_raw = env_var["tabela_escrita_raw"]

# endregion

#region Funcoes para apoio

def extracao_api(caminho_api):
    """
    Faz a conexão e extração dos produtos da API FakeStore.

    Parâmetros:
        caminho_api (str): Caminho(URL) para acessar a API.

    Retorno:
        json_produtos (dict): Dados dos produtos extraídos em formato JSON.
    """
    # Faz a requisição para a API
    response = requests.get(caminho_api)

    # Converte a resposta para JSON
    json_produtos = response.json()

    # Adiciona a data e hora do processamento ao JSON
    for produto in json_produtos:
        produto['dt_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Retorno dos dados em formato JSON
    return json_produtos

def inserir_no_bigquery(json_produtos, dataset_id_raw, tabela_id_raw):
    """
    Envia os dados do JSON para o BigQuery.

    Parâmetros:
        json_produtos (dict): Dados dos produtos extraídos em formato JSON.
        dataset_id_raw (str): Nome do dataset no BigQuery.
        tabela_id_raw (str): Nome da tabela no BigQuery.
    
    Retorno:
        Notificação de sucesso.
    """
    # Cria uma instância do cliente BigQuery
    client = bigquery.Client.from_service_account_json(variavel_acesso)

    # Defina o nome do dataset e tabela do BigQuery
    dataset_id = 'portfolio-440702.' + dataset_id_raw
    table_id = tabela_id_raw

    # Converte o JSON para um DataFrame
    df_produtos = pd.DataFrame(json_produtos)

    # Carregar os dados do DataFrame para o BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("image", "STRING"),
            bigquery.SchemaField("rating", "STRING"),
            bigquery.SchemaField("dt_processamento", "DATETIME"),
        ],
        write_disposition="WRITE_APPEND",
    )

    # Carregar os dados para o BigQuery
    client.load_table_from_dataframe(df_produtos, f'{dataset_id}.{table_id}', job_config=job_config).result()

    print(f'Dados inseridos com sucesso na tabela {dataset_id}.{table_id}')

#endregion

#region DAG

# Argumentos para a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

# Início da DAG
with DAG(
    'dag_pipe_produtos',
    default_args=default_args,
    schedule_interval=None
) as dag:

    #region RAW
    # Criando um TaskGroup chamado "raw"
    with TaskGroup("raw") as raw:

        # Task dentro do grupo "raw"
        python_extracao_dados_produtos = PythonOperator(
            task_id='python_extracao_dados_produtos',
            python_callable=extracao_api,
            op_kwargs={'caminho_api': variavel_api}
        )

        # Task para inserir no BigQuery
        python_inserir_bigquery = PythonOperator(
            task_id='python_inserir_bigquery',
            python_callable=inserir_no_bigquery,
            op_kwargs={'dataset_id_raw': variavel_camada_raw, 'tabela_id_raw': variavel_tabela_escrita_raw, 'json_produtos': '{{ task_instance.xcom_pull(task_ids="raw.python_extracao_dados_produtos") }}'}
        )
    
    #endregion

#endregion

#region Fluxo de tasks

    python_extracao_dados_produtos >> python_inserir_bigquery

#endregion
