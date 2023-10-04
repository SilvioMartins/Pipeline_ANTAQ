###########################################################
# OBJETIVO:  Carga Na Camada refined no Lake 
# DATA: 02Out2023
# OWNER: Silvio Martins
###########################################################

# Importanndo Bibliotecas
import sys
from operator import add
from pyspark.sql import SparkSession, Row
from datetime import datetime

# Criando Spark Session
spark = SparkSession\
   .builder\
   .appName("Processing_ANTAQ")\
   .getOrCreate()

# Criação de Tabelas no Hadoop
def create_tables_refined():
    # Cria/Trunca Tabela Atracacao_fato no Hadoop
    spark.sql('''
        CREATE TABLE IF NOT EXISTS refined.atracacao_fato (
            IdAtracacao                     string,
            CDTUP                           string,
            IdBerco                         string,
            Berco                           string,
            Porto_Atracacao                 string,
            Apelido_Instalacao_Portuaria    string,
            Complexo_Portuario              string,
            Tipo_Autoridade_Portuaria       string,
            Data_Atracacao                  string,
            Data_Chegada                    string,
            Data_Desatracacao               string,
            Data_Incio_Operacao             string,
            Data_Termino_Operacao           string,
            Tipo_Operacao                   string,
            Tipo_Navegacao_Atracacao        string,
            Nacionalidade_armador           string,
            FlagMCOperacaoAtracacao         string,            
            Terminal                        string,
            Municipio                       string,
            UF                              string,
            SGUF                            string,
            Regiao_Geografica               string,
            num_Capitania                   string,
            num_Imo                         string,
            TEsperaAtracacao                string,
            TesperaInicioOp                 string,
            TOperacao                       string,
            TEsperaDesatracacao             string,
            TAtracado                       string,
            TEstadia                        string )
        COMMENT 'Tabela refined.atracacao_fato'
        PARTITIONED BY(ano_data_inicio_operacao string, mes_data_inicio_operacao string)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';';
              
        TRUNCATE TABLE refined.atracacao_fato;
    ''')

    # Cria/Trunca Tabela Carga_fato no Hadoop
    spark.sql('''
        CREATE TABLE IF NOT EXISTS refined.carga_fato (   
            IdCarga                             string,
            IdAtracacao                         string,
            Origem                              string,
            Destino                             string,
            CDMercadoria                        string,
            Tipo_Operacao_Carga                 string,
            Carga_Geral_Acordicionamento        string,
            ConteinerEstado                     string,
            Tipo_Navegacao                      string,
            FlagAutorizacao                     string,
            FlagCabotagem                       string,
            FlagCabotagemMovimentacao           string,
            FlagConteinerTamanho                string,
            FlagLongoCurso                      string,
            FlagMCOperacaoCarga                 string,
            FlagOffshore                        string,                                
            FlagTransporteViaInterior           string,
            Percurso_Transporte_vias_Interiores string,
            Percurso_Transporte_Interiores      string,
            STNaturezaCarga                     string,
            StSh2                               string,
            StSh4                               string,
            Natureza_Carga                      string,              
            Sentido                             string,
            Teu                                 string,
            QtCarga                             string,
            VlPesoCargaBruta                    string,
            Porto_Atracacao                     string,
            SGUF                                string,
            Peso_liquido_carga                  string)
        COMMENT 'Tabela refined.carga_fato'
        PARTITIONED BY(ano_data_inicio_operacao string, mes_data_inicio_operacao string)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ';';
                     
        TRUNCATE TABLE processing.Carga;
    ''')

try:
    # Cria/Trunca Tabelas refined
    create_tables_refined()
    # Prepara e Carrega dados no Hadoop refined.atracacao_fato
    spark.sql(f'''
        INSERT INTO TABLE refined.atracacao_fato    
        SELECT 
            atrac.IdAtrac         as IdAtracacao,
            atrac.CDTUP           as CDTUP,
            atrac.IdBerco         as IdBerco,
            atrac.NomeBerco       as Berco,
            atrac.PortoAtrac      as Porto_Atracacao,
            atrac.ApelidoPorto    as Apelido_Instalacao_Portuaria,
            atrac.CompPorto       as Complexo_Portuario,
            atrac.Tipo Autor      as Tipo_Autoridade_Portuaria,
            atrac.DtAtrac         as Data_Atracacao,
            atrac.DtChegada       as Data_Chegada,
            atrac.DtDesatrac      as Data_Desatracacao,
            atrac.DtIniOper       as Data_Incio_Operacao,
            atrac.DtFimOper       as Data_Termino_Operacao,
            atrac.TipoOper        as Tipo_Operacao,
            atrac.TipoNavegOper   as Tipo_Navegacao_Atracacao,
            atrac.NacArmador      as Nacionalidade_armador,
            atrac.FlagMCOper      as FlagMCOperacaoAtracacao ,
            atrac.Terminal        as Terminal,
            atrac.Municipio       as Municipio,
            atrac.UF              as UF,
            atrac.SGUF            as SGUF,
            atrac.Regiao          as Regiao_Geografica ,
            atrac.IdCapit         as num_Capitania,
            atrac.IdImo           as num_Imo,
            temp.TEspAtracp       as TEsperaAtracacao,
            temp.TEspInicAtrac    as TesperaInicioOp ,
            temp.TOper            as TOperacao,
            temp.TEspDesatrac     as TEsperaDesatracacao,
            temp.TAtrac           as TAtracado,
            temp.TEstadia         as TEstadia,              
            atrac.ano             as ano_data_inicio_operacao,
            atrac.mes             as mes_data_inicio_operacao
        FROM processing.Atracacao atrac
        LEFT JOIN processing.TemposAtracacao temp
        on atrac.IdAtrac = temp.IdAtrac 
    ''')

        # Prepara e Carrega dados no Hadoop refined.carga_fato
    spark.sql(f'''
        INSERT INTO TABLE refined.carga_fato    
                                            
        SELECT 
            carga.IdCarga             as IdCarga,
            carga.IdAtrac             as IdAtracacao,
            carga.Origem              as Origem,
            carga.Destino             as Destino,
            carga.CDMerc              as CDMercadoria,
            carga.TipoOperCraga       as Tipo_Operacao_Carga,
            carga.CargaGeralAcord     as Carga_Geral_Acordicionamento,
            carga.Contestado          as ConteinerEstado,
            carga.TipoNaveg           as Tipo_Navegacao, 
            carga.FlagAutor           as FlagAutorizacao,
            carga.FlagCabot           as FlagCabotagem,
            carga.FlagCabotMov        as FlagCabotagemMovimentacao,
            carga.FlagCabotTam        as FlagConteinerTamanho,
            carga.FlagLongoPerc       as FlagLongoCurso,
            carga.FlagMco             as FlagMCOperacaoCarga,
            carga.FlagOff             as FlagOffshore,
            carga.FlagTranspViaInt    as FlagTransporteViaInterior,
            carga.PercTrViasInt       as Percurso_Transporte_vias_Interiores,
            carga.PercTranspInte      as Percurso_Transporte_Interiores,
            carga.StNatCarga          as STNaturezaCarga, 
            carga.StSh2               as StSh2,
            carga.StSh4               as StSh4,
            carga.NatCarga            as Natureza_Carga,
            carga.Sentido             as Sentido,
            carga.Teu                 as Teu,
            carga.QtCarga             as QtCarga,
            carga.VlPesoCargaBruta    as VlPesoCargaBruta,
            atrac.PortoAtrac          as Porto_Atracacao,
            atrac.SGUF                as SGUF,
            atrac.ano                 as ano_data_inicio_operacao,
            atrac.mes                 as mes_data_inicio_operacao,
            cont.VlPesoCargaCont      as Peso_liquido_carga
        FROM processing.carga carga
        LEFT JOIN processing.Carga_Conteinerizada cont
        ON carga.IdCarga = cont.IdCarga 
        LEFT JOIN processing.Atracacao atrac 
        ON carga.IdAtrac = atrac.IdAtrac
    ''')

except:
    print('Problemas com o Processamento!')