import pandera as pa

schema_empresa = pa.DataFrameSchema({
    "CNPJ BÁSICO": pa.Column(pa.String),
    "RAZÃO SOCIAL / NOME EMPRESARIAL": pa.Column(pa.String),
    "NATUREZA JURÍDICA": pa.Column(pa.String),
    "QUALIFICAÇÃO DO RESPONSÁVEL": pa.Column(pa.String),
    "CAPITAL SOCIAL DA EMPRESA": pa.Column(pa.Float),
    "PORTE DA EMPRESA": pa.Column(pa.String),
    "ENTE FEDERATIVO RESPONSÁVEL": pa.Column(pa.String)
})