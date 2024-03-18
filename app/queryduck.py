import duckdb
import os
import schemas


def define_tabelas(pasta_origem: str, final_arquivo: str , schema_modelo):
    """
        Função que recebe um caminho e devolve a definição de uma tabela:
        Args:
            pasta_origem (str) pasta onde estão os arquivos
            final_arquivo (str) extensão final do arquivo
            schema_modelo esquema da tabela
    """

    caminho_completo = os.path.join(pasta_origem, final_arquivo)

    tipo_substituicao = {
            'str': 'varchar',
            'float64': 'double',
            'datetime64[ns]': 'DATE'
    }
    tipos_das_colunas = {col: str(schema_modelo.columns[col].dtype).replace("DataType(", "").replace(")", "") for col in schema_modelo.columns}
    tipos_das_colunas = {col: tipo_substituicao.get(tipo, tipo) for col, tipo in tipos_das_colunas.items()}
    tabela_mounth = f"""read_csv('{caminho_completo}', decimal_separator=",", columns={tipos_das_colunas}, dateformat='%Y%m%d', ignore_errors=true)"""
    return tabela_mounth


if __name__ == "__main__":

    pasta_origem = 'dados/utf8/'
    tbl_empresas = define_tabelas(pasta_origem, '*.EMPRECSV', schemas.schema_empresa )
    tbl_estabelecimentos = define_tabelas(pasta_origem, '*.ESTABELE', schemas.schema_estabelecimento )
    tbl_simples = define_tabelas(pasta_origem, '*.D40210', schemas.schema_simples )
    tbl_socios = define_tabelas(pasta_origem, '*.SOCIOCSV', schemas.schema_socios)
    tbl_paises = define_tabelas(pasta_origem, '*.PAISCSV', schemas.schema_paises)
    tbl_municipios = define_tabelas(pasta_origem, '*.MUNICCSV', schemas.schema_municipios)
    tbl_qualificacoes_socios = define_tabelas(pasta_origem, '*.QUALSCSV', schemas.schema_qualificacoes_socios)
    tbl_natureza_jurifica = define_tabelas(pasta_origem, '*.NATJUCSV', schemas.schema_naturezas_juridicas)
    tbl_cnaes = define_tabelas(pasta_origem, '*.CNAECSV', schemas.schema_cnaes)
    


    query = f"""
            SELECT
            date_trunc('month',
            estabele.DATA_DE_INICIO_ATIVIDADE) AS MES_ANO_DE_INICIO_ATIVIDADE,
            date_trunc('month',
            estabele.DATA_SITUACAO_CADASTRAL) AS MES_ANO_SITUACAO_CADASTRAL,
            --cnaes.descricao,
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
            { tbl_estabelecimentos } estabele
        LEFT JOIN
            { tbl_cnaes } cnaes ON
            cnaes.CODIGO = estabele.CNAE_FISCAL_PRINCIPAL
        LEFT JOIN 
            { tbl_empresas } empresas ON
            empresas.CNPJ_BASICO = estabele.CNPJ_BASICO
        LEFT JOIN
             {tbl_natureza_jurifica } natureza ON
            empresas.NATUREZA_JURIDICA = natureza.CODIGO 
        GROUP BY
            situacao_cadastral,
            estabele.UF,
            cnaes.descricao,
            natureza.DESCRICAO,
            date_trunc('month',estabele.DATA_DE_INICIO_ATIVIDADE),
            date_trunc('month',estabele.DATA_SITUACAO_CADASTRAL),
            porte_empresa;
        """

    df_duckdb = duckdb.sql(query)

    print(df_duckdb)