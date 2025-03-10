#region Imports

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
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
variavel_models_trusted = env_var["models_trusted"]

credentials, project = google.auth.load_credentials_from_file(variavel_acesso)

# endregion

#region Funcoes

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
    
    ## CAMINHO ESSENCIAL - TRUNCATE (NAO FOI FEITO POR CONTA DO FREE TIER)
    #query = f"TRUNCATE TABLE `{variavel_camada_raw}.{dataset_id_raw}.{tabela_id_raw}`"
    #query_job = client.query(query)
    #query_job.result()
    #print(f"✅ Tabela {dataset_id_raw}.{tabela_id_raw} truncada com sucesso!")

    # Define a referência da tabela
    tabela_ref = client.dataset(dataset_id_raw).table(tabela_id_raw)

    # Configuração do Job para sobrescrever a tabela
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"  # Sobrescreve a tabela com os novos dados
    )

    # Insere o DataFrame no BigQuery
    job = client.load_table_from_dataframe(df, tabela_ref, job_config=job_config)


    # Aguarda a conclusão da inserção
    job.result()

    logging.info(f"✅ Dados inseridos com sucesso na tabela {dataset_id_raw}.{tabela_id_raw}")


def enviar_notificacao_slack(task_status, **kwargs):
    """
    Envia uma notificação para o Slack com o status da execução da task.

    Args:
        task_status (str): Status da task (ex: "sucesso" ou "falha").
        **kwargs: Argumentos adicionais.
    """
    dag_name = kwargs.get('dag').dag_id
    task_name = kwargs.get('task_instance').task_id
    execution_date = kwargs.get('execution_date')

    # Mensagem a ser enviada para o Slack
    if task_status == "sucesso":
        mensagem = f"✅ A dag_pipe_produtos foi executada com sucesso!\nExecução: {execution_date}"
    else:
        mensagem = f"❌ A dag_pipe_produtos falhou!\nExecução: {execution_date}"

    # Usando o SlackWebhookOperator para enviar a mensagem
    slack_notification = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id="slack_default",  # Você deve ter configurado essa conexão no Airflow com o webhook do Slack
        message=mensagem,
        channel="#notificação-vendas"
    )

    return slack_notification.execute(context=kwargs)

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

    #region Dummys
    inicio_dag = DummyOperator(
        task_id="inicio_dag"
    )

    fim_dag = DummyOperator(
        task_id="fim_dag"
    )
    #endregion

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
            },
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        # Fluxo de execução entre as tasks
        python_extracao_dados_produtos >> python_inserir_bigquery
    
    with TaskGroup("trusted") as trusted:

        # Tarefa para executar o DBT após a inserção no BigQuery
        dataquality_tb_raw_products = PythonOperator(
            task_id="dataquality_tb_raw_products",
            python_callable=utils.run_dbt_test,
            op_kwargs={
                'tabela_teste': variavel_tabela_escrita_raw
            },
            trigger_rule=TriggerRule.ALL_SUCCESS  # Garante que o DBT só será executado se a inserção no BigQuery for bem-sucedida
        )

        # Tarefa para executar o DBT após a inserção no BigQuery
        python_run_stg_products = PythonOperator(
            task_id="python_run_stg_products",
            python_callable=utils.run_dbt_tranformacao,
            op_kwargs={
                'models': variavel_models_trusted,
                'target': variavel_camada_trusted
            },
            trigger_rule=TriggerRule.ALL_SUCCESS  # Garante que o DBT só será executado se a inserção no BigQuery for bem-sucedida
        )

        python_inserir_bigquery >> dataquality_tb_raw_products >> python_run_stg_products

    #endregion

    #region Slack Notificacao
    with TaskGroup("slack") as slack:
        # Task de sucesso
        success_task = PythonOperator(
            task_id="success_task",
            python_callable=enviar_notificacao_slack,
            op_kwargs={'task_status': 'sucesso'},
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        # Task de falha
        failure_task = PythonOperator(
            task_id="failure_task",
            python_callable=enviar_notificacao_slack,
            op_kwargs={'task_status': 'falha'},
            trigger_rule=TriggerRule.ONE_FAILED
        )

    #endregion

    #region Fluxo

    inicio_dag >> raw >> trusted >> [success_task, failure_task] >> fim_dag

    #endregion

#endregion