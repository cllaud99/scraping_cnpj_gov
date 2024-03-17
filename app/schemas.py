import pandera as pa

schema_empresa = pa.DataFrameSchema({
    "CNPJ BÁSICO": pa.Column(pa.Int),
    "RAZÃO SOCIAL / NOME EMPRESARIAL": pa.Column(pa.String),
    "NATUREZA JURÍDICA": pa.Column(pa.Int),
    "QUALIFICAÇÃO DO RESPONSÁVEL": pa.Column(pa.Int),
    "CAPITAL SOCIAL DA EMPRESA": pa.Column(pa.Float),
    "PORTE DA EMPRESA": pa.Column(pa.Int),
    "ENTE FEDERATIVO RESPONSÁVEL": pa.Column(pa.String)
})

schema_estabelecimento = pa.DataFrameSchema({
    "CNPJ BÁSICO": pa.Column(pa.String),
    "CNPJ ORDEM": pa.Column(pa.String),
    "CNPJ DV": pa.Column(pa.String, nullable=False),
    "IDENTIFICADOR MATRIZ/FILIAL": pa.Column(pa.Int, pa.Check.isin([1, 2]), nullable=False),
    "NOME FANTASIA": pa.Column(pa.String),
    "SITUAÇÃO CADASTRAL": pa.Column(pa.String, nullable=False, checks=pa.Check.isin(["01", "02", "03", "04", "08"])),
    "DATA SITUAÇÃO CADASTRAL": pa.Column(pa.DateTime),
    "MOTIVO SITUAÇÃO CADASTRAL": pa.Column(pa.String),
    "NOME DA CIDADE NO EXTERIOR": pa.Column(pa.String),
    "PAIS": pa.Column(pa.String),
    "DATA DE INÍCIO ATIVIDADE": pa.Column(pa.DateTime),
    "CNAE FISCAL PRINCIPAL": pa.Column(pa.String),
    "CNAE FISCAL SECUNDÁRIA": pa.Column(pa.String),
    "TIPO DE LOGRADOURO": pa.Column(pa.String),
    "LOGRADOURO": pa.Column(pa.String),
    "NÚMERO": pa.Column(pa.String),
    "COMPLEMENTO": pa.Column(pa.String),
    "BAIRRO": pa.Column(pa.String),
    "CEP": pa.Column(pa.String),
    "UF": pa.Column(pa.String, pa.Check.str_length(2, 2), nullable=False),
    "MUNICÍPIO": pa.Column(pa.String),
    "DDD 1": pa.Column(pa.String),
    "TELEFONE 1": pa.Column(pa.String),
    "DDD 2": pa.Column(pa.String),
    "TELEFONE 2": pa.Column(pa.String),
    "DDD DO FAX": pa.Column(pa.String),
    "FAX": pa.Column(pa.String),
    "CORREIO ELETRÔNICO": pa.Column(pa.String),
    "SITUAÇÃO ESPECIAL": pa.Column(pa.String),
    "DATA DA SITUAÇÃO ESPECIAL": pa.Column(pa.DateTime)
})

schema_simples = pa.DataFrameSchema({
    "CNPJ BÁSICO": pa.Column(pa.String),
    "OPÇÃO PELO SIMPLES": pa.Column(pa.String, nullable=True),
    "DATA DE OPÇÃO PELO SIMPLES": pa.Column(pa.DateTime, nullable=True),
    "DATA DE EXCLUSÃO DO SIMPLES": pa.Column(pa.DateTime, nullable=True),
    "OPÇÃO PELO MEI": pa.Column(pa.String, nullable=True),
    "DATA DE OPÇÃO PELO MEI": pa.Column(pa.DateTime, nullable=True),
    "DATA DE EXCLUSÃO DO MEI": pa.Column(pa.DateTime, nullable=True)
})



schema_socios = pa.DataFrameSchema({
    "CNPJ BÁSICO": pa.Column(pa.String),
    "IDENTIFICADOR DE SÓCIO": pa.Column(pa.String),
    "NOME DO SÓCIO (NO CASO PF) OU RAZÃO SOCIAL (NO CASO PJ)": pa.Column(pa.String),
    "CNPJ/CPF DO SÓCIO": pa.Column(pa.String),
    "QUALIFICAÇÃO DO SÓCIO": pa.Column(pa.String),
    "DATA DE ENTRADA SOCIEDADE": pa.Column(pa.DateTime),
    "PAIS": pa.Column(pa.String),
    "REPRESENTANTE LEGAL": pa.Column(pa.String),
    "NOME DO REPRESENTANTE": pa.Column(pa.String),
    "QUALIFICAÇÃO DO REPRESENTANTE LEGAL": pa.Column(pa.String),
    "FAIXA ETÁRIA": pa.Column(pa.String)
})

# Schema para as qualificações de sócios
schema_qualificacoes_socios = pa.DataFrameSchema({
    "CÓDIGO": pa.Column(pa.String),
    "DESCRIÇÃO": pa.Column(pa.String)
})

# Schema para as naturezas jurídicas
schema_naturezas_juridicas = pa.DataFrameSchema({
    "CÓDIGO": pa.Column(pa.String),
    "DESCRIÇÃO": pa.Column(pa.String)
})

# Schema para os CNAEs
schema_cnaes = pa.DataFrameSchema({
    "CÓDIGO": pa.Column(pa.String),
    "DESCRIÇÃO": pa.Column(pa.String)
})
