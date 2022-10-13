import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

raw = "/tmp/tabela_unica.csv"

default_args = {
    'owner': "Gregori",
    'depends_on_past': False,
    'start_date': datetime (2022, 10, 9),
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'], description = 'TRABALHO 2 - DAG 2')
def trabalho2_dag2():

    @task
    def gera_resultados():
        NOME_DO_ARQUIVO = "/tmp/resultados.csv"
        df = pd.read_csv(raw, sep=';')

        df_media = pd.DataFrame()
        df_media['media_qtd_passageiros'] = [df['qtd_passageiros'].mean()]
        df_media['media_tarifa'] = [df['tarifa'].mean()]
        df_media['media_qtd_familiares'] = [df['qtd_famil'].mean()]
        
        print ("\n"+df_media.to_string())
        df_media.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        
        return NOME_DO_ARQUIVO


    resultado = gera_resultados()
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    resultado >> end

execucao = trabalho2_dag2()