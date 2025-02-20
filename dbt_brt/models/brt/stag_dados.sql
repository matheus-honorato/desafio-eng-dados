{{ config(
    alias='stag_dados', 
) }}


WITH origem_transformada as (
SELECT  
    data_extracao,
	TIMESTAMP 'epoch' + ((dados_array->>'dataHora')::BIGINT / 1000) * INTERVAL '1 second' AS dataHora,
    dados_array->>'codigo' AS id_onibus,
    dados_array->>'latitude' latitude,
    dados_array->>'longitude' longitude,
    dados_array->>'velocidade' velocidade
FROM raw_brt.dados_api,
LATERAL jsonb_array_elements(dados) AS dados_array)
  
  SELECT 
      id_onibus, 
      latitude, 
      longitude, 
      velocidade
  FROM origem_transformada