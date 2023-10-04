
###########################################################
# OBJETIVO:  Construção Pipeline airflow
# DATA: 02Out2023
# OWNER: Silvio Martins 
###########################################################

#Importanndo Bibliotecas
import datetime
from sys import api_version
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
   
#Parâmetros da DAG
default_args = {
    'owner': 'Silvio Martins',
    'start_date': days_ago(1),
    'email': ['suport@sfiec.org.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

#Construção DAG
with DAG(
    'ETL_ANTAQ',
    default_args=default_args,
    description='Desafio Observatório da Indústria - Engenharia de Dados',
    schedule_interval=datetime.timedelta(days=1),
    max_active_runs=1
) as dag:

   #Task INGESTÃO DE ARQUIVOS
    Ingestion = BashOperator(
        task_id="010_Ingestion",
        bash_command= "python 010_Ingestio.py",
        dag = dag,
    )

   #Task PROCESSAMENTO NA CAMADA PROCESSING
    Processing = BashOperator(
        task_id="020_Processing",
        bash_command= "python 020_Processing.py",
        dag = dag,
    )

   #Task Camada REFINED
    Refined = BashOperator(
        task_id="030_Refined",
        bash_command= "python 030_Refined.py",
        dag = dag,
    )

   #Task EXPORT To SQl SERVER
    ExportSQL = BashOperator(
        task_id="040_Export",
        bash_command= "python 040_Export.py",
        dag = dag,
    )

    #Task  Email Sucesso Processo
    EmailSucess = EmailOperator(
        task_id="Email_Sucess",
        to= "suport@sfiec.org.br",
        subject = 'ANTAQ Flow finished sucessfull',
        html_content = ''' <h1>  Congratulations, the ANTAQ Flow finished sucessfull! ''',
        dag = dag,
    )
Ingestion >> Processing >> Refined >> ExportSQL >> EmailSucess