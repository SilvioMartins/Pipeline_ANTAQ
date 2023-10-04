###########################################################
# OBJETIVO:  Exportação da Tabela para SQl Server 
# DATA: 02Out2023
# OWNER: Silvio Martins
###########################################################

# Importanndo Bibliotecas
import os

# Variáveis
PASSWORD='Senha'
SQL_SERVER="Host SQL Server"
DATABASE="Database"
SERVER_DB_CONNECT="jdbc:sqlserver://$SQL_SERVER.database.windows.net:1433;user=sqluser;password=$PASSWORD;database=$DABATASE"
SQOOP_ATRACACAO = '''sqoop export --connect $SERVER_DB_CONNECT \
                            -table atracacao_fato \
                            --hcatalog-table refined.atracacao_fato'''
SQOOP_CARGA = '''sqoop export --connect $SERVER_DB_CONNECT \
                            -table carga_fato \
                            --hcatalog-table refined.carga_fato'''

# Exportação das Tabelas para SQL Server
try:
    # Exportação Table ATRACACAO para SQL SERVER
    os.system(SQOOP_ATRACACAO)
    # Exportação Table CARGA para SQL SERVER
    os.system(SQOOP_CARGA)
except:
    print('Problema na Exportação Tabelas SQL Server.')