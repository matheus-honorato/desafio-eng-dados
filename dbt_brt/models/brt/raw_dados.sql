{{ config(materialized='ephemeral') }}

with source_data as (

    SELECT
        id,
        data_extracao,
        dados
    FROM raw_brt.dados_api

)

select *
from source_data