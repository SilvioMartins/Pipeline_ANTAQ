###########################################################
# OBJETIVO:  Carga e tratamento Camada Processing Lake
# DATA: 02Out2023
# OWNER: Silvio Martins
###########################################################

# Importanndo Bibliotecas
import sys
from operator import add
from pyspark.sql import SparkSession, Row
from datetime import datetime

#Variáveis 
# Ano Atual
CY = datetime.now().year
# Determinação dos 3 últimos anos móveis - RN.1
anos = [str(CY-2),str(CY-1),str(CY)]
# Nomes dos Arquivos
fnames = ['Atracacao', 'TemposAtracacao', 'Carga', 'Carga_Conteinerizada']
# Criando Spark Session
spark = SparkSession\
   .builder\
   .appName("Processing_ANTAQ")\
   .getOrCreate()

# Criação de Tabelas no Hadoop
def create_tables_procesing():
    # Cria/Trunca Tabela Atracacao no Hadoop
    spark.sql('''
        CREATE TABLE IF NOT EXISTS processing.Atracacao (
            IdAtrac         string,
            CDTUP           string,
            IdBerco         string,
            NomeBerco       string,
            PortoAtrac      string,
            ApelidoPorto    string,
            CompPorto       string,
            TipoAutor       string,
            DtAtrac         string,
            DtChegada       string,
            DtDesatrac      string,
            DtIniOper       string,
            DtFimOper       string,
            TipoOper        string,
            TipoNavegOper   string,
            NacArmador      string,
            FlagMCOper      string,
            Terminal        string,
            Municipio       string,
            UF              string,
            SGUF            string,
            Regiao          string,
            IdCapit         string,
            IdImo           string)
        COMMENT 'Tabela processing.Atracacao'
        PARTITIONED BY(Ano string, Mes string)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';' STORED AS Parquet;
              
        TRUNCATE TABLE processing.Atracacao;
    ''')

    # Cria/Trunca Tabela TemposAtracacao no Hadoop
    spark.sql('''
        CREATE TABLE IF NOT EXISTS processing.TemposAtracacao (              
            IdAtrac         string,
            TEspAtracp      string,
            TEspInicAtrac   string,
            TOper           string,
            TEspDesatrac    string,
            TAtrac          string,
            TEstadia        string)
        COMMENT 'Tabela processing.TemposAtracacao'
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';' STORED AS Parquet;
              
        TRUNCATE TABLE processing.TemposAtracacao;
    ''')

    # Cria/Trunca Tabela Carga no Hadoop
    spark.sql('''
        CREATE TABLE IF NOT EXISTS processing.Carga (   
            IdCarga             string,
            IdAtrac             string,
            Origem              string,
            Destino             string,
            CDMerc              string,
            TipoOperCraga       string,
            CargaGeralAcord     string,
            Contestado          string,
            TipoNaveg           string,
            FlagAutor           string,
            FlagCabot           string,
            FlagCabotMov        string,
            FlagCabotTam        string,
            FlagLongoPerc       string,
            FlagMco             string,
            FlagOff             string,
            FlagTranspViaInt    string,
            PercTrViasInt       string,
            PercTranspInte      string,
            StNatCarga          string,
            StSh2               string,
            StSh4               string,
            NatCarga            string,
            Sentido             string,
            Teu                 string,
            QtCarga             string,
            VlPesoCargaBruta    string)
        COMMENT 'Tabela processing.Carga'
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';' STORED AS Parquet;
                     
        TRUNCATE TABLE processing.Carga;
    ''')

    # Cria/Trunca Tabela Carga_Conteinerizada no Hadoop
    spark.sql('''
        CREATE TABLE IF NOT EXISTS processing.Carga_Conteinerizada (   
            IdCarga             string,
            CDmercCont          string,
            VlPesoCargaCont     string) 
        COMMENT 'Tabela processing.Carga_Conteinerizada'
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';' STORED AS Parquet;
              
        TRUNCATE TABLE processing.Carga_Conteinerizada;
    ''')


try:
    # Rotina de Criação das Tabelas em Processing
    create_tables_procesing()
    # Analisa os anos de processamento
    for ano in anos:
        # Pega os nomes dos arquivos .TXT
        for f_name in fnames:
            # Carrega os .TXT nas tabelas Hadoop
            spark.sql(f'''
                INSERT INTO TABLE processing.{f_name} 
                SELECT
                FROM 
                      ''')

            # Carrega o .TXT no Dataframe
            df = spark.read.options(header='True', inferSchema='True', delimiter=';') \
                 .csv(f'{ano}{f_name}.txt') 
            # Ajusta os Nomes das Colunas
            if f_name == 'Atracacao':
                lst_col = ['IdAtrac', 'CDTUP', 'IdBerco', 'NomeBerco', 'PortoAtrac', 'ApelidoPorto',
                           'CompPorto', 'Tipo Autor', 'DtAtrac', 'DtChegada', 'DtDesatrac', 'DtIniOper',
                           'DtFimOper', 'Ano', 'Mes', 'TipoOper', 'TipoNavegOper', 'NacArmador', 'FlagMCOper', 
                           'Terminal', 'Municipio', 'UF', 'SGUF', 'Regiao', 'IdCapit', 'IdImo']
                new_df = df.toDF(*lst_col)
                # Cria View Temporária
                new_df.createOrReplaceTempView(f'{f_name}')
                # Carrega os .TXT nas tabelas Hadoop
                spark.sql(f'''
                    INSERT INTO TABLE processing.{f_name} 
                    SELECT * FROM {f_name}'
                ''')
            elif f_name == 'Carga':  
                lst_col = ['IdCarga', 'IdAtrac', 'Origem', 'Destino', 'CDMerc', 'TipoOperCraga',
                           'CargaGeralAcord', 'Contestado', 'TipoNaveg', 'FlagAutor', 'FlagCabot',
                           'FlagCabotMov', 'FlagCabotTam', 'FlagLongoPerc', 'FlagMco', 'FlagOff', 'FlagTranspViaInt', 
                           'PercTrViasInt ', 'PercTranspInte', 'StNatCarga', 'StSh2', 'StSh4', 'NatCarga', 'Sentido', 'Teu', 
                           'QtCarga', 'VlPesoCargaBruta']
                new_df = df.toDF(*lst_col)
                # Cria View Temporária
                new_df.createOrReplaceTempView(f'{f_name}')
                # Carrega os .TXT nas tabelas Hadoop
                spark.sql(f'''
                    INSERT INTO TABLE processing.{f_name} 
                    SELECT * FROM {f_name}'
                ''')
            elif f_name == 'Carga_Conteinerizada':  
                lst_col = ['IdCarga', 'CDmercCont', 'VlPesoCargaCont' ]
                new_df = df.toDF(*lst_col)
                # Cria View Temporária
                new_df.createOrReplaceTempView(f'{f_name}')
                # Carrega os .TXT nas tabelas Hadoop
                spark.sql(f'''
                    INSERT INTO TABLE processing.{f_name} 
                    SELECT * FROM {f_name}'
                ''')
            elif f_name == 'TemposAtracacao':  
                lst_col = ['IdAtrac', 'TEspAtracp', 'TEspInicAtrac', 'TOper', 'TEspDesatrac',
                           'TAtrac', 'TEstadia'] 
                new_df = df.toDF(*lst_col)
                # Cria View Temporária
                new_df.createOrReplaceTempView(f'{f_name}')
                # Carrega os .TXT nas tabelas Hadoop
                spark.sql(f'''
                    INSERT INTO TABLE processing.{f_name} 
                    SELECT * FROM {f_name}'
                ''')  

except:
    print('Problemas com o Processamento!')
