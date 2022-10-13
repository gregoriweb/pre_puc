import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"
raw = "/tmp/titanic.csv"

default_args = {
    'owner': "Gregori",
    'depends_on_past': False,
    'start_date': datetime (2022, 10, 9),
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'], description = 'TRABALHO 2 - DAG 1')
def trabalho2_dag1():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = raw
        df = pd.read_csv(URL, sep=';')

        df.rename(columns={
            'PassengerId': 'id_passageiro',     
            'Survived'   : 'sobrevivente',
            'Pclass'     : 'classe',
            'Name'       : 'nome',
            'Sex'        : 'sexo',
            'Age'        : 'idade',
            'SibSp'      : 'irmaos_conjuges',
            'Parch'      : 'pais_filhos',
            'Ticket'     : 'numero_passagem',
            'Fare'       : 'tarifa',
            'Cabin'      : 'cabine',
            'Embarked'   : 'portao_embarque'
            }, inplace=True)
            
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")

        print ("\n"+df.head(20).to_string())
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(nome_do_arquivo):
       NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
       df = pd.read_csv(nome_do_arquivo, sep=";")
       df.rename(columns={'id_passageiro':'qtd_passageiros'}, inplace=True)
       res = df.groupby(['sexo', 'classe']).agg({
                    "qtd_passageiros": "count"
                }).reset_index()

       print("\n"+res.to_string())
       res.to_csv(NOME_TABELA, index=False, sep=";")
       return NOME_TABELA

    @task
    def ind_tarifa(nome_do_arquivo):
        NOME_TABELA = "/tmp/tarifa_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['sexo', 'classe']).agg({
            "tarifa": "mean"
        }).reset_index()
        print("\n"+res.to_string())
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA
 
    @task
    def ind_familiares(nome_do_arquivo):
        NOME_TABELA = "/tmp/familiares_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df["qtd_famil"] = df["irmaos_conjuges"] + df["pais_filhos"] 
        res = df.groupby(['sexo', 'classe']).agg({
            "qtd_famil": "sum"
        }).reset_index()
        print("\n"+res.to_string())
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA
 
    @task
    def tabela_unica():
        raw_passageiros = "/tmp/passageiros_por_sexo_classe.csv"
        raw_tarifa = "/tmp/tarifa_por_sexo_classe.csv"
        raw_familiares = "/tmp/familiares_por_sexo_classe.csv"
        
        NOME_TABELA = "/tmp/tabela_unica.csv"
 
        passageiros = pd.read_csv(raw_passageiros, sep=";")
        tarifa = pd.read_csv(raw_tarifa, sep=";")
        familiares = pd.read_csv(raw_familiares, sep=";")
 
        df_resultado = (
            passageiros
                .merge(tarifa, how="inner", on=['sexo','classe'])
                .merge(familiares, how="inner", on=['sexo','classe'])
        ).reset_index()
 
        print("\n"+df_resultado.to_string())
        
        df_resultado.to_csv(NOME_TABELA, index=False, sep=";")
 
        return NOME_TABELA
 

    ing = ingestao()
    indicador = ind_passageiros(raw)
    tarifa = ind_tarifa(raw)
    familiares = ind_familiares(raw)
    resultados = tabela_unica()
    trigger_dag2 = TriggerDagRunOperator(
      task_id="inicia_dag2",
      trigger_dag_id="trabalho2_dag2"
    )

    ing >> indicador >> tarifa >> familiares >> resultados >> trigger_dag2

execucao = trabalho2_dag1()