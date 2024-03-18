import pandera as pa

schema_empresa = pa.DataFrameSchema({
    "CNPJ_BASICO": pa.Column(pa.Int),
    "RAZAO_SOCIAL_NOME_EMPRESARIAL": pa.Column(pa.String),
    "NATUREZA_JURIDICA": pa.Column(pa.Int),
    "QUALIFICACAO_DO_RESPONSAVEL": pa.Column(pa.Int),
    "CAPITAL_SOCIAL_DA_EMPRESA": pa.Column(pa.Float),
    "PORTE_DA_EMPRESA": pa.Column(pa.Int),
    "ENTE_FEDERATIVO RESPONSÁVEL": pa.Column(pa.String)
})

schema_estabelecimento = pa.DataFrameSchema({
    "CNPJ_BASICO": pa.Column(pa.String),
    "CNPJ_ORDEM": pa.Column(pa.String),
    "CNPJ_DV": pa.Column(pa.String, nullable=False),
    "IDENTIFICADOR_MATRIZ_FILIAL": pa.Column(pa.Int, pa.Check.isin([1, 2]), nullable=False),
    "NOME_FANTASIA": pa.Column(pa.String),
    "SITUACAO_CADASTRAL": pa.Column(pa.String, nullable=False, checks=pa.Check.isin(["01", "02", "03", "04", "08"])),
    "DATA_SITUACAO_CADASTRAL": pa.Column(pa.DateTime),
    "MOTIVO_SITUACAO_CADASTRAL": pa.Column(pa.String),
    "NOME_DA_CIDADE_NO_EXTERIOR": pa.Column(pa.String),
    "PAIS": pa.Column(pa.String),
    "DATA_DE_INICIO_ATIVIDADE": pa.Column(pa.DateTime),
    "CNAE_FISCAL_PRINCIPAL": pa.Column(pa.String),
    "CNAE_FISCAL_SECUNDARIA": pa.Column(pa.String),
    "TIPO_DE_LOGRADOURO": pa.Column(pa.String),
    "LOGRADOURO": pa.Column(pa.String),
    "NUMERO": pa.Column(pa.String),
    "COMPLEMENTO": pa.Column(pa.String),
    "BAIRRO": pa.Column(pa.String),
    "CEP": pa.Column(pa.String),
    "UF": pa.Column(pa.String, pa.Check.str_length(2, 2), nullable=False),
    "MUNICIPIO": pa.Column(pa.String),
    "DDD_1": pa.Column(pa.String),
    "TELEFONE_1": pa.Column(pa.String),
    "DDD_2": pa.Column(pa.String),
    "TELEFONE_2": pa.Column(pa.String),
    "DDD_DO_FAX": pa.Column(pa.String),
    "FAX": pa.Column(pa.String),
    "CORREIO_ELETRONICO": pa.Column(pa.String),
    "SITUACAO_ESPECIAL": pa.Column(pa.String),
    "DATA_DA_SITUACAO_ESPECIAL": pa.Column(pa.DateTime)
})

schema_simples = pa.DataFrameSchema({
    "CNPJ_BASICO": pa.Column(pa.String),
    "OPCAO_PELO_SIMPLES": pa.Column(pa.String, nullable=True),
    "DATA_DE_OPCAO_PELO_SIMPLES": pa.Column(pa.DateTime, nullable=True),
    "DATA_DE_EXCLUSAO_DO_SIMPLES": pa.Column(pa.DateTime, nullable=True),
    "OPCAO_PELO_MEI": pa.Column(pa.String, nullable=True),
    "DATA_DE_OPCAO_PELO_MEI": pa.Column(pa.DateTime, nullable=True),
    "DATA_DE_EXCLUSAO_DO_MEI": pa.Column(pa.DateTime, nullable=True)
})

schema_socios = pa.DataFrameSchema({
    "CNPJ_BASICO": pa.Column(pa.String),
    "IDENTIFICADOR_DE_SOCIO": pa.Column(pa.String),
    "NOME_DO_SOCIO_NO_CASO_PF_OU_RAZAO_SOCIAL_NO_CASO_PJ": pa.Column(pa.String),
    "CNPJ_CPF_DO_SOCIO": pa.Column(pa.String),
    "QUALIFICACAO_DO_SOCIO": pa.Column(pa.String),
    "DATA_DE_ENTRADA_SOCIEDADE": pa.Column(pa.DateTime),
    "PAIS": pa.Column(pa.String),
    "REPRESENTANTE_LEGAL": pa.Column(pa.String),
    "NOME_DO_REPRESENTANTE": pa.Column(pa.String),
    "QUALIFICACAO_DO_REPRESENTANTE_LEGAL": pa.Column(pa.String),
    "FAIXA_ETARIA": pa.Column(pa.String)
})

# Schema para as qualificações de sócios
schema_qualificacoes_socios = pa.DataFrameSchema({
    "CODIGO": pa.Column(pa.String),
    "DESCRICAO": pa.Column(pa.String)
})

# Schema para as naturezas jurídicas
schema_naturezas_juridicas = pa.DataFrameSchema({
    "CODIGO": pa.Column(pa.String),
    "DESCRICAO": pa.Column(pa.String)
})

# Schema para os CNAEs
schema_cnaes = pa.DataFrameSchema({
    "CODIGO": pa.Column(pa.String),
    "DESCRICAO": pa.Column(pa.String)
})

# Schema para os países
schema_paises = pa.DataFrameSchema({
    "CODIGO": pa.Column(pa.String),
    "DESCRICAO": pa.Column(pa.String)
})

# Schema para os municípios
schema_municipios = pa.DataFrameSchema({
    "CODIGO": pa.Column(pa.String),
    "DESCRICAO": pa.Column(pa.String)
})
