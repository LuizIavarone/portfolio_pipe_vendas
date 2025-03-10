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
import google.auth
from airflow.utils.dates import days_ago
import logging
import ast

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

credentials, project = google.auth.load_credentials_from_file(variavel_acesso)

# endregion

#region Funcoes para apoio

def extracao_api(caminho_api, **kwargs):
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

    logging.info(f"Conteúdo de json_produtos: {json_produtos}")

    # Armazenando o valor no XCom para ser recuperado por outra task
    kwargs['ti'].xcom_push(key='json_produtos', value=json_produtos)

    # Retorno dos dados em formato JSON
    return json_produtos

def inserir_no_bigquery(json_produtos, dataset_id_raw, tabela_id_raw):
    """
    Insere os dados de produtos no BigQuery.

    Args:
        json_produtos (dict): Os dados dos produtos extraídos da API.
        dataset_id_raw (str): O nome do dataset no BigQuery onde os dados serão inseridos.
        tabela_id_raw (str): O nome da tabela no BigQuery onde os dados serão inseridos.

    Raises:
        ValueError: Se os dados estiverem vazios ou ocorrerem erros na inserção no BigQuery.
    """

    # Substitui as chaves que têm aspas extras e as corrige
    data_string = json_produtos.replace("'id'", '"id"').replace("'title'", '"title"').replace("'price'", '"price"').replace("'description'", '"description"').replace("'category'", '"category"').replace("'image'", '"image"').replace("'rating'", '"rating"').replace("'dt_processamento'", '"dt_processamento"')
    
    # Usando ast.literal_eval para transformar a string em uma lista de dicionários
    json_produtos = ast.literal_eval(data_string)

    # Converte o JSON em um DataFrame
    df = pd.json_normalize(json_produtos)

    # Renomeando as colunas que possuem nomes compostos
    df.columns = df.columns.str.replace('rating.rate', 'rating_rate', regex=False)
    df.columns = df.columns.str.replace('rating.count', 'rating_count', regex=False)
    
    # Convertendo colunas para STRING para corresponder ao esquema do BigQuery
    df = df.astype({
        'id': 'string',  
        'price': 'string',  
        'rating_rate': 'string',
        'rating_count': 'string',
        'dt_processamento': 'string'
    })

    # Autenticação com BigQuery
    client = bigquery.Client(credentials=credentials, project=project)

    # Define a referência da tabela
    tabela_ref = client.dataset(dataset_id_raw).table(tabela_id_raw)

    # Insere o DataFrame no BigQuery
    job = client.load_table_from_dataframe(df, tabela_ref)

    # Aguarda a conclusão da inserção
    job.result()

    logging.info(f"✅ Dados inseridos com sucesso na tabela {dataset_id_raw}.{tabela_id_raw}")



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
            op_kwargs={
                'dataset_id_raw': variavel_camada_raw, 
                'tabela_id_raw': variavel_tabela_escrita_raw,
                'json_produtos': '{{ task_instance.xcom_pull(task_ids="raw.python_extracao_dados_produtos", key="json_produtos") }}'
            }
        )

        # Fluxo de execução entre as tasks
        python_extracao_dados_produtos >> python_inserir_bigquery
    #endregion

#endregion