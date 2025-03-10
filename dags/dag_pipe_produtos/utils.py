#region Imports

import json
import os
import subprocess

#endregion

def run_dbt_test(tabela_teste):
    """
        Executa o ambiente DBT

        Parâmetros:
            tabela_teste (str): Nome da tabela para testes.
        
        Retorno:
            Sem retorno, a execucao iniciar o projeto DBT para execucao.
    """
    command = f"bash -c 'source /home/admin/airflow_env/bin/activate && cd /home/admin/Projetos/pipe_vendas/dbt_pipe_vendas && dbt test --models {tabela_teste}'"
    subprocess.run(command, shell=True, check=True)

def run_dbt_tranformacao(models,target):
    """
        Executa o ambiente DBT

        Parâmetros:
            target (str): Nome do ambiente configurado no profile que deseja acessar.
        
        Retorno:
            Sem retorno, a execucao iniciar o projeto DBT para execucao.
    """
    command = f"bash -c 'source /home/admin/airflow_env/bin/activate && cd /home/admin/Projetos/pipe_vendas/dbt_pipe_vendas && dbt run --models {models} --target {target}'"

    subprocess.run(command, shell=True, check=True)

def get_variable(file_name, deserialize_json=False):
    """
    Carrega as variáveis de um arquivo JSON.

    Parâmetros:
        file_name (str): Nome do arquivo JSON (sem precisar adicionar .json manualmente).
        deserialize_json (bool): Se True, converte o conteúdo de JSON para um objeto Python.

    Retorno:
        O dicionário do arquivo JSON.
    """
    # Garante que o nome do arquivo tenha a extensão .json
    if not file_name.endswith(".json"):
        file_name += ".json"

    # Obtém o caminho completo do arquivo (assumindo que está na mesma pasta)
    file_path = os.path.join(os.path.dirname(__file__), file_name)

    try:
        # Carrega o conteúdo do JSON
        with open(file_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"O arquivo {file_name} não foi encontrado no caminho {file_path}.")
    
    return data  # Retorna o dicionário inteiro