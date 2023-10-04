with dados_Ceara as (
    select 
        uf                                              as Localidade,
        ano_data_inicio_operacao                        as ano,
        mes_data_inicio_operacao                        as mes,
        count(*)                                        as num_atracacao,
        avg(cast(TEsperaAtracacao as decimal(10,3)))    as TesperaMedio,
        avg(cast(TAtracado as decimal(10,2)))           as TAtracadoMedio
    from atracacao_fato
    where uf = 'CE'
    group by 
        uf,
        ano_data_inicio_operacao,
        mes_data_inicio_operacao
),
with dados_Nordeste as (
    select 
        Regiao_Geografica                               as Localidade,
        ano_data_inicio_operacao                        as ano,
        mes_data_inicio_operacao                        as mes,
        count(*)                                        as num_atracacao,
        avg(cast(TEsperaAtracacao as decimal(10,3)))    as TesperaMedio,
        avg(cast(TAtracado as decimal(10,2)))           as TAtracadoMedio
    from atracacao_fato
    where Regiao_Geografica = 'Nordeste'
    group by 
        Regiao_Geografica,
        ano_data_inicio_operacao,
        mes_data_inicio_operacao
),
with dados_Brasil as (
    select 
        'Brasil'                                        as Localidade,
        ano_data_inicio_operacao                        as ano,
        mes_data_inicio_operacao                        as mes,
        count(*)                                        as num_atracacao,
        avg(cast(TEsperaAtracacao as decimal(10,3)))    as TesperaMedio,
        avg(cast(TAtracado as decimal(10,2)))           as TAtracadoMedio
    from atracacao_fato
    group by 
        ano_data_inicio_operacao,
        mes_data_inicio_operacao
)

select 
    Localidade,
    ano,
    mes,
    num_atrac,
    TesperaMedio,
    TAtracadoMedio
from (
    select  Localidade,  ano, mes, num_atrac, TesperaMedio,  TAtracadoMedio from dados_Ceara
    union all
    select  Localidade,  ano, mes, num_atrac, TesperaMedio,  TAtracadoMedio from dados_Nordeste
    union all
    select  Localidade,  ano, mes, num_atrac, TesperaMedio,  TAtracadoMedio from dados_Brasil
)
