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