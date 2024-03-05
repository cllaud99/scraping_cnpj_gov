import pandera as pa


# Define a função de validação para o CNPJ BÁSICO
def validate_cnpj(cnpj):
    if len(cnpj) != 8:
        raise ValueError("O CNPJ BÁSICO deve ter 8 caracteres.")

# Define a função de validação para o PORTE DA EMPRESA
def validate_porte(porte):
    valid_values = {"00", "01", "03", "05"}
    if porte not in valid_values:
        raise ValueError("O PORTE DA EMPRESA deve ser '00', '01', '03' ou '05'.")

schema = pa.DataFrameSchema({
    "CNPJ BÁSICO": pa.Column(pa.String, 
                              pa.Check(validate_cnpj), 
                              nullable=False),
    "RAZÃO SOCIAL / NOME EMPRESARIAL": pa.Column(pa.String, 
                                                  pa.Check(lambda x: len(x) <= 100), 
                                                  nullable=False),
    "NATUREZA JURÍDICA": pa.Column(pa.String, 
                                   pa.Check(lambda x: len(x) <= 50), 
                                   nullable=False),
    "QUALIFICAÇÃO DO RESPONSÁVEL": pa.Column(pa.String, 
                                              pa.Check(lambda x: len(x) <= 50), 
                                              nullable=False),
    "CAPITAL SOCIAL DA EMPRESA": pa.Column(pa.Float, 
                                           nullable=False),
    "PORTE DA EMPRESA": pa.Column(pa.String, 
                                   pa.Check(validate_porte), 
                                   nullable=False),
    "ENTE FEDERATIVO RESPONSÁVEL": pa.Column(pa.String, 
                                              pa.Check(lambda x: len(x) <= 50), 
                                              nullable=True)
})