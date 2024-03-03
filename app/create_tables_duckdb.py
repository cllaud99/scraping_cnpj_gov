import duckdb

DATABASE_NAME = 'gov_cnpj.db'


con = duckdb.connect(DATABASE_NAME)

con.sql("""
CREATE TABLE cnaes (
                CNAE_FISCAL_PRINCIPAL VARCHAR ,
                DESCRICAO VARCHAR ,
);


CREATE TABLE natureza_juridica (
                NATUREZA_JURIDICA VARCHAR ,
                DESCRICAO VARCHAR ,
);


CREATE TABLE qualificacao_socio (
                QUALIFICACAO_DO_SOCIO VARCHAR ,
                DESCRICAO VARCHAR ,
);


CREATE TABLE municipios (
                MUNICIPIO VARCHAR ,
                DESCRICAO VARCHAR ,
);


CREATE TABLE paises (
                PAIS VARCHAR ,
                DESCRICAO VARCHAR ,
);


CREATE TABLE empresas (
                CNPJ_BASICO VARCHAR ,
                RAZAO_SOCIAL_NOME_EMPRESARIAL VARCHAR ,
                NATUREZA_JURIDICA VARCHAR ,
                QUALIFICACAO_DO_RESPONSAVEL VARCHAR ,
                CAPITAL_SOCIAL_DA_EMPRESA VARCHAR ,
                PORTE_DA_EMPRESA VARCHAR ,
                ENTE_FEDERATIVO_RESPONSAVEL VARCHAR
);


CREATE TABLE socios (
                CNPJ_BASICO VARCHAR ,
                IDENTIFICADOR_DE_SOCIO VARCHAR ,
                NOME_DO_SOCIO_NO_CASO_PF_OU_RAZAO_SOCIAL_NO_CASO_PJ VARCHAR ,
                CNPJ_CPF_DO_SOCIO VARCHAR ,
                QUALIFICACAO_DO_SOCIO VARCHAR ,
                DATA_DE_ENTRADA_SOCIEDADE VARCHAR ,
                PAIS VARCHAR ,
                REPRESENTANTE_LEGAL VARCHAR ,
                NOME_DO_REPRESENTANTE VARCHAR ,
                QUALIFICACAO_DO_REPRESENTANTE_LEGAL VARCHAR ,
                FAIXA_ETARIA VARCHAR 
);


CREATE TABLE simples (
                CNPJ_BASICO VARCHAR ,
                OPCAO_PELO_SIMPLES VARCHAR ,
                DATA_DE_OPCAO_PELO_SIMPLES VARCHAR,
                DATA_DE_EXCLUSAO_DO_SIMPLES VARCHAR,
                OPCAO_PELO_MEI VARCHAR,
                DATA_DE_OPCAO_PELO_MEI VARCHAR,
                DATA_DE_EXCLUSAO_DO_MEI VARCHAR
);


CREATE TABLE estabelecimentos (
                CNPJ_BASICO VARCHAR ,
                CNPJ_ORDEM VARCHAR ,
                CNPJ_DV VARCHAR ,
                IDENTIFICADOR_MATRIZ_FILIAL VARCHAR ,
                NOME_FANTASIA VARCHAR ,
                SITUACAO_CADASTRAL VARCHAR ,
                DATA_SITUACAO_CADASTRAL VARCHAR ,
                MOTIVO_SITUACAO_CADASTRAL VARCHAR ,
                NOME_DA_CIDADE_NO_EXTERIOR VARCHAR ,
                PAIS VARCHAR ,
                DATA_DE_INICIO_ATIVIDADE VARCHAR ,
                CNAE_FISCAL_PRINCIPAL VARCHAR ,
                CNAE_FISCAL_SECUNDARIA VARCHAR ,
                TIPO_DE_LOGRADOURO VARCHAR ,
                LOGRADOURO VARCHAR ,
                NUMERO VARCHAR ,
                COMPLEMENTO VARCHAR ,
                BAIRRO VARCHAR ,
                CEP VARCHAR ,
                UF VARCHAR ,
                MUNICIPIO VARCHAR ,
                DDD_1 VARCHAR ,
                TELEFONE_1 VARCHAR ,
                DDD_2 VARCHAR ,
                TELEFONE_2 VARCHAR ,
                DDD_DO_FAX VARCHAR ,
                FAX VARCHAR ,
                CORREIO_ELETRONICO VARCHAR ,
                SITUACAO_ESPECIAL VARCHAR ,
                DATA_DA_SITUACAO_ESPECIAL VARCHAR 
);
""")