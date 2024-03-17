SELECT
	date_trunc('month',
	estabele.DATA_DE_INICIO_ATIVIDADE) AS MES_ANO_DE_INICIO_ATIVIDADE,
	date_trunc('month',
	estabele.DATA_SITUACAO_CADASTRAL) AS MES_ANO_SITUACAO_CADASTRAL,
	cnaes.descricao,
	estabele.UF,
	CASE
		WHEN estabele.SITUACAO_CADASTRAL = '02' THEN 'ATIVA'
		WHEN estabele.SITUACAO_CADASTRAL = '03' THEN 'SUSPENSA'
		WHEN estabele.SITUACAO_CADASTRAL = '04' THEN 'INAPTA'
		WHEN estabele.SITUACAO_CADASTRAL = '08' THEN 'BAIXADA'
		ELSE 'OUTROS'
	END AS situacao_cadastral,
	CASE
		WHEN empresas.PORTE_DA_EMPRESA  = 1 THEN 'Micro Empresa'
		WHEN estabele.SITUACAO_CADASTRAL = 3 THEN 'Empresa Pequeno Porte'
		ELSE 'Demais'
	END AS porte_empresa,
	natureza.DESCRICAO AS natureza_juridica,
	COUNT(*) AS qtd_empresas
FROM
	main.estabelecimentos estabele
LEFT JOIN
    main.cnaes cnaes ON
	cnaes.CODIGO = estabele.CNAE_FISCAL_PRINCIPAL
LEFT JOIN 
	main.empresas empresas ON
	empresas.CNPJ_BASICO = estabele.CNPJ_BASICO
LEFT JOIN
	main.naturezas_juridicas natureza ON
	empresas.NATUREZA_JURIDICA = natureza.CODIGO 
GROUP BY
	situacao_cadastral,
	estabele.UF,
	cnaes.descricao,
	natureza.DESCRICAO,
	date_trunc('month',estabele.DATA_DE_INICIO_ATIVIDADE),
	date_trunc('month',estabele.DATA_SITUACAO_CADASTRAL),
	porte_empresa;