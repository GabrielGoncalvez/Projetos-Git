import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta
from alive_progress import alive_bar
import time
import re
import sys  # Importa o módulo sys para encerrar o script
from openpyxl import load_workbook
import cx_Oracle as oracle
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus as urlquote
import numpy as np
import glob
import warnings
from colorama import Fore, Style, init
from sas7bdat import SAS7BDAT
import xlsxwriter
print('26/02-v3')

print("""
-------------------->> CONTROLTECH <<--------------------
AUTOR: --
DATA DE CRIAÇÃO: 2025.05
""")

# Cores para o print
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"
WHITE = "\033[37m"

# Registrar o tempo de início
start_time = time.time()

# Configuração do ambiente Oracle
try:
    # set the environment variables
    print("\nConfigurando as variáveis de ambiente...")
    os.environ['OCI_INC'] = r'\\172.22.0.33\Departamentos\Controladoria\aplicações\instantclient_21_11\sdk\include'
    os.environ['OCI_LIB64'] = r'\\172.22.0.33\Departamentos\Controladoria\aplicações\instantclient_21_11'
    os.environ['PATH'] = r'\\172.22.0.33\Departamentos\Controladoria\aplicações\instantclient_21_11;' + os.environ['PATH']
    # os.environ['ORA_TZFILE'] = r'\\172.22.0.33\Departamentos\Controladoria\aplicações\instantclient_21_11\timezone_18.dat'
    # Apontamentos na minha máquina, para quando não tiver acesso na rede, ajuste necessário também na variacoes do ambiente
    # os.environ['OCI_LIB64'] = r'C:\oracle\instantclient_23_6'p
    # os.environ['PATH'] = r'C:\oracle\instantclient_23_6;' + os.environ['PATH']
    print(f"{GREEN}Variáveis de ambiente configuradas com sucesso!")
except Exception as e:
    print(f"{RED}Falha ao configurar as variáveis de ambiente: %s" % e)


# Variáveis de configuração
variaveis_de_configuracao = {
    'DT_INI': '01/01/2024',  # DATA INICIAL DO PERÍODO DE ATUALIZAÇÃO
    'DT_FIM': '31/01/2025'  # DATA FINAL DO PERÍODO DE ATUALIZAÇÃO
}


# Filtros para Extração (direto no formato correto)
variaveis_para_extracao = {
    'DT_INI_PARA_EXTRACAO': '2024-08-01',  # Data inicial no formato YYYY-MM-DD
    'DT_FIM_PARA_EXTRACAO': '2025-08-13',   # Data final no formato YYYY-MM-DD
    'EMPRESA_PARA_EXTRACAO': ['74', '139']
}


# Registrar o tempo de término
end_time = time.time()
elapsed_time = end_time - start_time

# Exibir o tempo total de execução
print(f"{BLUE}Tempo total de execução: {elapsed_time:.2f} segundos")

# Variáveis de controle
variaveis_de_controle = {
    # DEPARA e BASES COMPLEMENTARES
    'importa_deparas': False,
    'importa_base_centro_ebs': False,
    # BASE BALANCETE EBS
    'lista_balancete_ebs': False,
    'importa_balancete_ebs': False,
    # BASE RAZÃO EBS
    'importa_razao_ebs': False,
    'update_razao_ebs': False,
    # BASE PAC
    'importa_base_pac': False,
    'update_base_pac': True,
    'ajuste_manual_pac': True,
    # CALCULOS CPV
    'calcula_cpv': False,
    # CALCULOS COMPRAS PAC
    'calcula_compras_pac': False,
    # CALCULOS COMPRAS CPV
    'calcula_compras_cpv': False,
    # DEMONSTRATIVO CPV
    'Primeiro_Demonstrativo_CPV': True,
    'Segundo_Demonstrativo_CPV': True,
    # BASES ANALITICAS
    'exporta_base_analitica': False,
    'exporta_base_analitica_PAC': False,
    'exporta_base_analitica_GRADES': False,
    'exporta_base_analitica_RAZAO': False,
    # VALIDAÇÃO CMV
    'validacao_cmv': False,
    # SAS
    'importa_sas': False
}

try:
    # Criar o DataFrame de mapeamento
    month_map = pd.DataFrame({
        'PERIODO': ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'],
        'MONTH_NUM': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    })

    # Gerar a parte da query SQL para a conversão de PERIODO para DATA_BASE
    case_statements = "CASE "
    for _, row in month_map.iterrows():
        case_statements += f"WHEN substr(PERIODO, 1, 3) = '{row['PERIODO']}' THEN '{row['MONTH_NUM']}' "
    case_statements += "END"

    # Parte da query SQL completa
    data_base_conversion = f"'01' || {case_statements} || '20' || substr(PERIODO, 5, 2) AS DATA_BASE"

except Exception as e:
    print(f"{RED}Erro na padronização de database: {e}")


# Caminho do banco de dados
pasta_banco_dados = os.path.join(
    os.path.expanduser('~'),
    'OneDrive - EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
    '02- CONTROLADORIA',
    '01- BANCO_CPV_SQLITE'
)

# Conecte-se ao banco de dados SQLite se não exister crie um novoz/
conn = sqlite3.connect(os.path.join(pasta_banco_dados, 'CPV.sqlite'))
print(conn)

# Define o NLS_LANG antes de criar a conexão
os.environ["NLS_LANG"] = "BRAZILIAN PORTUGUESE_BRAZIL.AL32UTF8"

# Configurações de conexão Oracle (TP215)
TP215_db_user = 'APPSELECT'
TP215_db_pass = 'ORACLE123'
TP215_db_name = 'TP215'
TP215_db_port = 1521
TP215_db_host = 'TP215.ORACLEDB'
TP215_dsn_tns = oracle.makedsn(
    TP215_db_host, TP215_db_port, service_name=TP215_db_name)


# Cria a conexão com o Oracle usando SQLAlchemy
conn_TP215 = create_engine(f"oracle+cx_oracle://{TP215_db_user}:%s@{
                           TP215_dsn_tns}" % urlquote(TP215_db_pass), arraysize=10000, pool_size=0)


# Testa a conexão com o banco de dados Oracle
try:
    conn_TP215.connect()
    print(f"{GREEN}Conexão com o banco de dados Oracle estabelecida com sucesso!")
except Exception as e:
    print(f"{RED}Falha ao conectar ao banco de dados Oracle: %s" % e)
    sys.exit(1)  # Encerra o script em caso de falha

# Criação das tabelas de depara
if variaveis_de_controle['importa_deparas']:

    print(f"{BLUE}Importa Deparas")
    # Caminho da pasta contendo os arquivos de depara
    pasta_depara = os.path.join(
        os.path.expanduser('~'),
        'EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
        'ControlTech - Documentos',
        '07 - ControlTech',
        '01 - Projetos',
        '14 - CPV',
        '02 - Bases Depara')

    try:
        # Lista todos os arquivos na pasta
        for arquivo in os.listdir(pasta_depara):
            # Verifica se o arquivo é um arquivo Excel (.xlsx)
            if arquivo.endswith('.xlsx'):
                caminho_completo = os.path.join(pasta_depara, arquivo)

                # Leia o arquivo Excel sem considerar nenhuma linha como cabeçalho e pula a primeira linha do arquivo
                depara = pd.read_excel(caminho_completo)

                # Usa o nome do arquivo (sem a extensão) como o nome da tabela
                nome_tabela = os.path.splitext(arquivo)[0]

                # Insere os dados no banco de dados, substituindo a tabela se ela já existir
                depara.to_sql(nome_tabela, conn,
                              if_exists='replace', index=False)

                print(f"{GREEN}Tabela {nome_tabela} criada com sucesso.")

    except Exception as e:
        print(f"{RED}Erro ao processar os arquivos: {e}")

# Importa o arquivo de base de centros EBS -- ARQUIVO UNICO DO TIME DE CONTROLADORIA
if variaveis_de_controle['importa_base_centro_ebs']:

    print(f"{BLUE}importa Base de Centros EBS")
    # Caminho das bases de centro
    pasta_variavel = os.path.join(
        os.path.expanduser('~'),
        'EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
        'Base de Centros - ORACLE e SAP - Documentos'
    )

    # Listar os diretórios dentro da pasta anterior
    diretorios = [d for d in os.listdir(pasta_variavel) if os.path.isdir(
        os.path.join(pasta_variavel, d))]

    # Supondo que você queira o primeiro diretório encontrado
    nome_pasta_variavel = diretorios[0]

    # Caminho da base de centros
    pasta_base_centros = os.path.join(pasta_variavel, nome_pasta_variavel)

    # Caminho do arquivo da base de centros SAP
    arquivo_especifico = os.path.join(
        pasta_base_centros, 'Base de Centros - EBS.xlsb')

    # Verificar se o arquivo de base de centros EBS existe
    if os.path.isfile(arquivo_especifico):
        print(f"{GREEN}Arquivo encontrado: {arquivo_especifico}")
    else:
        print(f"{RED}Arquivo não encontrado")

    try:
        # Leitura do arquivo de base de centro EBS
        df_centros_ebs = pd.read_excel(arquivo_especifico, engine='pyxlsb')

        # Renomear a coluna 'CENTRO EBS' para 'CENTRO_EBS'
        df_centros_ebs.rename(
            columns={'CENTRO EBS': 'CENTRO_EBS'}, inplace=True)

        # Insere os registros no banco de dados
        df_centros_ebs.to_sql('BASE_CENTROS_EBS', conn, if_exists='replace',
                              index=True, dtype={'CENTRO': 'INTEGER'})

        print(f"{GREEN}Base de Centros EBS importada com sucesso!")

    except Exception as e:
        print(f"{RED}Erro ao importar a base de centros EBS: {e}")

    # Cria índice para a coluna CENTRO na tabela BASE_CENTROS_SAP
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_centro_ebs ON BASE_CENTROS_EBS(CENTRO_EBS);")
    conn.commit()

# Lista os arquivos do Balancete EBS --  Apenas para saber quais arquivos estão disponíveis para o balancete
if variaveis_de_controle['lista_balancete_ebs']:

    print(f"{BLUE}Lista Balancete EBS")

   # Caminho da rede dos arquivos estoque acabados
    pasta_importar = os.path.join(
        os.path.expanduser('~'),
        'EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
        'ControlTech - Documentos',
        '07 - ControlTech',
        '01 - Projetos',
        '14 - CPV',
        '04 - Base Balancete EBS'
    )

    # pasta_importar = os.path.join(
    #         r'\\172.22.0.33',
    #         'Controladoria',
    #         '_Exercicio 2023',
    #         '34 - Somos',
    #         '3 - Bases Relatorio CPV',
    #         '2. Base Grades',
    #     )

    # Função para converter o período de mmm/yy para mm/yyyy
    def converter_periodo(periodo):
        return datetime.strptime(periodo, '%b/%y').strftime('%m/%Y')

    # Lista para armazenar informações dos arquivos
    lista_balancete_ebs = pd.DataFrame(columns=[
                                       'caminho', 'nome_arquivo', 'tamanho_arquivo', 'data_modificacao', 'data_criacao', 'data_atualizacao', 'data_arquivo'])

    # Calcula o total de arquivos .txt para definir o total da barra de progresso
    total_files = sum([len(files) for r, d, files in os.walk(
        pasta_importar) if any(file.endswith('.xlsx') for file in files)])

    print(Fore.GREEN, end='')  # Define a cor verde para o texto
    with alive_bar(total_files, title="Lista Balancete EBS") as bar:

        # Percorra todos os arquivos na pasta
        for root, dirs, files in os.walk(pasta_importar):
            for file in files:
                if file.endswith('.xlsx'):
                    file_path = os.path.join(root, file)
                    file_size = os.path.getsize(file_path)
                    file_mtime = os.path.getmtime(file_path)
                    file_ctime = os.path.getctime(file_path)

                    # Ler o arquivo Excel para extrair o campo Período
                    df = pd.read_excel(file_path)
                    if 'Período' in df.columns:
                        periodo = df['Período'].iloc[0]
                        periodo = periodo.to_pydatetime().strftime('%b/%y')
                        data_arquivo = datetime.strptime(
                            periodo, '%b/%y').strftime('%m/%Y')
                    else:
                        data_arquivo = None

                    # Adicione as informações do arquivo na lista
                    lista_balancete_ebs = pd.concat([
                        lista_balancete_ebs,
                        pd.DataFrame({'caminho': [file_path],
                                      'nome_arquivo': [file],
                                      'tamanho_arquivo': [file_size],
                                      'data_modificacao': [datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')],
                                      'data_criacao': [datetime.fromtimestamp(file_ctime).strftime('%Y-%m-%d %H:%M:%S')],
                                      'data_atualizacao': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                                      'data_arquivo': [data_arquivo]})], ignore_index=False)
                    bar()  # Atualiza a barra de progresso

    # Salva o dataframe no sqlite
    lista_balancete_ebs.to_sql(
        'LISTA_BALANCETE_EBS', conn, if_exists='replace', index=False)

# Importa o Balancete EBS -- Importa os arquivos para tabela de balancete, que não tenha os memso MES_ANO já inseridos
if variaveis_de_controle['importa_balancete_ebs']:

    # Garante que a lista de balancetes seja carregada antes do uso
    lista_balancete_ebs = pd.read_sql(
        "SELECT * FROM LISTA_BALANCETE_EBS", conn)

    print(f"{BLUE}Importa Balancete EBS")

    # Define o período de datas
    data_inicio = datetime.strptime(
        variaveis_de_configuracao['DT_INI'], '%d/%m/%Y')
    data_fim = datetime.strptime(
        variaveis_de_configuracao['DT_FIM'], '%d/%m/%Y')

    # Filtra os arquivos dentro do intervalo de datas
    lista_balancete_ebs['data_base_date'] = pd.to_datetime(
        lista_balancete_ebs['data_arquivo'], format='%m/%Y')
    lista_balancete_ebs_filtro = lista_balancete_ebs[(lista_balancete_ebs['data_base_date'] >= data_inicio) & (
        lista_balancete_ebs['data_base_date'] <= data_fim)]

    # Calcula total de arquivos que serão importados
    total_files = lista_balancete_ebs_filtro.shape[0]

    print(Fore.GREEN, end='')  # Define a cor verde para o texto
    with alive_bar(total_files, title="Importando Balancete EBS") as bar:

        for index, row in lista_balancete_ebs_filtro.iterrows():
            # Leitura do arquivo
            if os.path.isfile(row['caminho']) and row['caminho'].endswith('.xlsx'):
                try:

                    df = pd.read_excel(row['caminho'], engine='openpyxl')

                except Exception as e:
                    print(f"{RED}Erro ao ler o arquivo {row['caminho']}: {e}")
            else:
                print(f"{RED}Erro: Arquivo {
                      row['caminho']} não encontrado ou não é um arquivo .xlsx válido.")

            # Usa o campo data_arquivo do DataFrame
            data_arquivo = row['data_arquivo']

            # Adiciona a data de atualização
            df['DATA_ATUALIZACAO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Adiciona o campo MES_ANO com o valor de data_arquivo
            df['MES_ANO'] = data_arquivo

            # Adiciona o campo ANO extraindo o ano de data_arquivo
            df['ANO'] = data_arquivo.split('/')[1]

            # Adiciona o campo MES extraindo o mês de data_arquivo
            df['MES'] = data_arquivo.split('/')[0]

            # Adiciona o campo DATA_BASE formato yyyy-mm-dd
            df['DATA_BASE'] = pd.to_datetime(
                data_arquivo, format='%m/%Y').strftime('%Y-%m-%d')

            # Renomeia várias colunas de uma vez
            df.rename(columns={
                'Conta': 'CONTA',
                'Saldo Final': 'SALDO_FINAL',
                'Empresa': 'EMPRESA',
                'Subconta': 'SUBCONTA'}, inplace=True)

            # Adiciona campo CHAVE concatenando os campos CONTA e SUBCONTA
            df['CHAVE'] = df['CONTA'].astype(str) + df['SUBCONTA'].astype(str)

            # Multiplica o campo SALDO_FINAL por -1 (EBS o valor vem com o sinal invertido)
            # df['SALDO_FINAL'] = df['SALDO_FINAL'] * -1

           # Remove caracteres não numéricos e espaços em branco da coluna 'EMPRESA'
            df['EMPRESA'] = df['EMPRESA'].str.replace(
                r'\D', '', regex=True).str.strip()

            # Converte a coluna 'EMPRESA' para inteiro
            df['EMPRESA'] = df['EMPRESA'].astype(int)

            # Ler a tabela DEPARA_EMPRESAS do banco de dados
            depara_empresas = pd.read_sql_query(
                "SELECT EMPRESA FROM DEPARA_EMPRESAS", conn)

            # Filtrar o DataFrame para manter apenas as empresas que estão na tabela DEPARA_EMPRESAS
            df = df[df['EMPRESA'].isin(depara_empresas['EMPRESA'])]

            # Verifica se a tabela existe
            table_exists = conn.execute(
                """SELECT name FROM sqlite_master WHERE type='table' AND name='BALANCETE_EBS';""").fetchone()

            if table_exists:

                # Deleta registros existentes com a mesma data_base
                mes_ano_value = df['MES_ANO'].iloc[0]

                delete_query = """DELETE FROM BALANCETE_EBS
                                WHERE MES_ANO = ?"""
                conn.execute(delete_query, (mes_ano_value,))
                conn.commit()
                print(f"{GREEN}Registros deletados da tabela BALANCETE_EBS: {
                      mes_ano_value}")

            else:
                pass

            try:
                # Insere os registros no banco de dados
                df.to_sql('BALANCETE_EBS', conn, if_exists='append',
                          index=False, dtype={'DATA_ATUALIZACAO': 'DATETIME'})
                bar()  # Atualiza a barra de progresso

            except Exception as e:
                print(f"{RED}Erro ao importar o arquivo {row['caminho']}: {e}")

# Importa a base de Razão Ativos EBS  -- PELO ORACLE, ATUALIZA ARQUIVOS E DELETA DATAS DUPLICADAS
if variaveis_de_controle['importa_razao_ebs']:

    print(f"{BLUE}Importa Razão EBS")

    # Obtenha as listas de empresas e contas
    empresas = pd.read_sql_query("""SELECT EMPRESA 
                                    FROM DEPARA_EMPRESAS""", conn)['EMPRESA'].tolist()

    # Completa o campo EMPRESA para ter até 3 caracteres com 0 à esquerda
    empresas = [str(empresa).zfill(3) for empresa in empresas]

    contas = pd.read_sql_query("""SELECT DISTINCT CONTA 
                                    FROM DEPARA_CONTA""", conn)['CONTA'].tolist()

    # Query para extrair os dados do Oracle
    base_query = """
                   SELECT
                   LTRIM(o105514.EMPRESA, '0') AS EMPRESA
                    ,o105514.CONTA
                    ,o105514.SUBCONTA
                    ,o105514.DATA_BASE
                    ,o105514.VLR_MOEDA_NACIONAL
                    ,o105514.VLR_MOEDA_ORIG
                    ,o105514.DT_ATUALIZACAO
                    ,o105514.HISTORICO
                    ,o105514.ORIGEM
                    ,o105514.CENTRO_CUSTO_LUCRO
                    ,o105514.LANCAMENTO
                    ,o105514.DESCRICAO
                    ,o105514.LIVRO
                    ,o105514.LOTE
            FROM ( SELECT  gjst.user_je_source_name
                ,gcc.segment1 EMPRESA
                ,gcc.segment2 CONTA
                ,gjb.NAME AS LOTE
                ,ffv.description DESCRICAO
                ,gcc.segment3 SUBCONTA
                ,gcc.segment4 AS CENTRO_CUSTO_LUCRO
                ,gjh.name AS LANCAMENTO
                ,gjst.user_je_source_name AS ORIGEM
                ,trunc(gjl.effective_date) DATA_BASE
                ,nvl(decode(nvl(gjl.accounted_dr,0),0,nvl(gjl.accounted_cr,0)*-1,nvl(gjl.accounted_dr,0)),0) AS VLR_MOEDA_NACIONAL
                ,decode(gjh.currency_code,'BRL',0,decode(nvl(gjl.entered_dr, 0), 0, nvl(gjl.entered_cr, 0) * -1, gjl.entered_dr)) AS VLR_MOEDA_ORIG
                ,gjh.last_update_date AS DT_ATUALIZACAO
                ,gjl.ledger_id LIVRO
                ,REPLACE(REPLACE(REPLACE(gjl.description, chr(10), ''), chr(13), ''),chr(9),'') AS HISTORICO

            FROM gl.gl_je_batches           gjb
                ,gl.gl_je_headers           gjh
                ,gl.gl_je_lines             gjl
                ,gl.gl_je_sources_tl        gjst
                ,gl.gl_je_categories_tl     gjct
                ,gl.gl_code_combinations    gcc
                ,gl.gl_ledgers              gb
                ,gl.gl_periods              gp
                ,APPLSYS.fnd_flex_value_sets ffvs
                ,apps.fnd_flex_values_vl ffv
            WHERE ffvs.flex_value_set_name = 'ABRL_GL_CONTA'
            AND ffv.flex_value_set_id = ffvs.flex_value_set_id
            AND gjb.je_batch_id = gjh.je_batch_id
            AND gjb.default_period_name = gjh.period_name
            AND gjh.je_source = gjst.je_source_name
            AND gjh.je_category = gjct.je_category_name
            AND gjh.je_header_id = gjl.je_header_id
            AND gjl.code_combination_id = gcc.code_combination_id
            AND gjh.ledger_id = gb.ledger_id
            AND gb.period_set_name = gp.period_set_name
            AND gp.period_name = gjh.period_name
            AND gcc.segment2 = ffv.flex_value
            AND gjh.actual_flag = 'A'
            AND gp.end_date >= gjl.effective_date
            AND gp.start_date <= gjl.effective_date
            AND gjst.LANGUAGE = userenv('LANG')
            AND gjct.LANGUAGE = userenv('LANG')
            ) o105514
            WHERE 1=1
            AND o105514.LIVRO = '2021' /*Livro para Ativos*/
            AND o105514.SUBCONTA BETWEEN '000' AND '999'
            AND o105514.CENTRO_CUSTO_LUCRO BETWEEN '000000' AND '999999'
            AND o105514.DATA_BASE BETWEEN TO_DATE(:DT_INI, 'DD/MM/YYYY') AND TO_DATE(:DT_FIM, 'DD/MM/YYYY') + 0.99999
            AND TRUNC(o105514.DT_ATUALIZACAO) BETWEEN TO_DATE(:DT_INI, 'DD/MM/YYYY')  AND ADD_MONTHS(TO_DATE(:DT_FIM, 'DD/MM/YYYY'), 12)
            """

    # Inicialize as variáveis de contagem
    total_inseridos = 0
    total_deletados = 0

    try:
        # Extrair os dados do Oracle
        print(Fore.GREEN, end='')  # Define a cor verde para o texto
        with alive_bar(title=f"Extraindo dados do Razão EBS") as bar:
            # Iterar sobre cada combinação de empresa e conta
            for empresa in empresas:
                for conta in contas:
                    # Adicionar filtros de empresa e conta à query base
                    select_query = base_query + \
                        f" AND o105514.EMPRESA = '{empresa}' AND o105514.CONTA = '{conta}'"

                    razao_ebs = pd.read_sql_query(select_query, conn_TP215, params={'DT_INI': variaveis_de_configuracao['DT_INI'],
                                                                                    'DT_FIM': variaveis_de_configuracao['DT_FIM']})
                    bar()

                    # Coloca os nomes das colunas em maiúsculo
                    razao_ebs.columns = razao_ebs.columns.str.upper()

                    # Converte a coluna DATA_BASE para o formato datetime
                    razao_ebs['DATA_BASE'] = pd.to_datetime(
                        razao_ebs['DATA_BASE'], format='%d%b%Y')

                    # Cria coluna MES_ANO (MM/YYYY) a partir da coluna DATA_BASE
                    razao_ebs['MES_ANO'] = razao_ebs['DATA_BASE'].dt.strftime(
                        '%m/%Y')

                    # Adiciona a coluna DATA_ATUALIZACAO com a data e hora atual
                    razao_ebs['DATA_ATUALIZACAO'] = datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')

                    # Cria coluna CLASSIFICACAO vazia
                    razao_ebs['CLASSIFICACAO'] = ''

                    # Verifica se a tabela existe
                    table_exists = conn.execute(
                        """SELECT name FROM sqlite_master WHERE type='table' AND name='RAZAO_EBS';""").fetchone()

                    if table_exists:

                        # Converte as datas de configuração para objetos datetime
                        DT_INI = datetime.strptime(
                            variaveis_de_configuracao['DT_INI'], '%d/%m/%Y')
                        DT_FIM = datetime.strptime(
                            variaveis_de_configuracao['DT_FIM'], '%d/%m/%Y')

                        # Deleta da tabela razao_ebs os registros que estão sendo importados
                        try:
                            deletados = conn.execute("""
                                DELETE FROM RAZAO_EBS
                                WHERE DATA_BASE BETWEEN :DT_INI AND :DT_FIM
                                AND EMPRESA = :EMPRESA
                                AND CONTA = :CONTA
                            """, {
                                'DT_INI': DT_INI.strftime('%Y-%m-%dT%H:%M:%S'),
                                'DT_FIM': DT_FIM.strftime('%Y-%m-%dT%H:%M:%S'),
                                'EMPRESA': empresa,
                                'CONTA': conta
                            }).rowcount
                            conn.commit()

                            # Atualiza a contagem de deletados
                            total_deletados += deletados

                        except Exception as e:
                            print(f"{RED}Erro ao deletar registros: {e}")

                    else:
                        pass

                    # Insere os dados do Kardex no sqlite
                    razao_ebs.to_sql('RAZAO_EBS', conn, if_exists='append', index=False, dtype={
                                     'DATA_ATUALIZACAO': 'DATETIME'})

                    # Atualiza a contagem de inseridos
                    total_inseridos += len(razao_ebs)

        # Imprime as contagens totais
        print(f"{GREEN}Total de registros deletados Razão EBS: {total_deletados}")
        print(f"{GREEN}Total de registros inseridos Razão EBS: {total_inseridos}")

    except Exception as e:
        print(f"{RED}Erro ao processar os dados: {e}")

# Gera e atualiza a tabela Razão EBS Total  -- PELO ORACLE UPDATE DA RAZAO - AJUSTANDO CLASSIFICAÇÃO
if variaveis_de_controle['update_razao_ebs']:

    print(f"{BLUE}Atualiza a tabela Razão EBS")

    try:
        # Define a query de atualização para a coluna CLASSIFICACAO
        update_query = """
            UPDATE RAZAO_EBS
            SET CLASSIFICACAO = (
                CASE
                    WHEN HISTORICO LIKE '%GLOBAL TECH%' AND ORIGEM IN ('Contas a Pagar', 'CLL F189 INTEGRATED RCV') THEN 'SERVIÇOS'
                    WHEN ORIGEM = 'Inventário Periódico' AND CONTA = '1140401003' AND EMPRESA = 35 THEN 'Reclassificações/Outros'
                    WHEN ORIGEM = 'Inventário Periódico' THEN 'INV'
                    WHEN SUBCONTA = '455' THEN 'AVP'
                    WHEN ORIGEM = 'PAC' THEN 'PAC'
                    WHEN ORIGEM IN ('Contas a Pagar', 'CLL F189 INTEGRATED RCV') THEN 'COMPRAS'
                    WHEN CONTA IN ('1140201004', '3210190001', '3210190002') THEN 'COMPRAS'
                    ELSE 'Reclassificações/Outros'
                END
            )
        """
        conn.execute(update_query)
        conn.commit()
        print(f"{GREEN}Coluna CLASSIFICACAO atualizada com sucesso.")

    except Exception as e:
        print(f"{RED}Erro ao atualizar a coluna CLASSIFICACAO: {e}")

# Importa a base de  --   Atualizando base PAC -- PELO ORACLE Inseindo dados por conta e empresa equivale as bases sas CPV_BI.PAC_ORIG - flow macro do sas
if variaveis_de_controle['importa_base_pac']:

    print(f"{BLUE}Importa Base PAC")

    # Obtenha as listas de empresas e contas
    empresas = pd.read_sql_query("""SELECT EMPRESA FROM DEPARA_EMPRESAS""", conn)[
        'EMPRESA'].tolist()

    # Completa o campo EMPRESA para ter até 3 caracteres com 0 à esquerda
    empresas = [str(empresa).zfill(3) for empresa in empresas]

    contas = pd.read_sql_query("""SELECT DISTINCT CONTA FROM DEPARA_CONTA""", conn)[
        'CONTA'].tolist()

    contas_formatadas = ', '.join(str(conta) for conta in contas)

    # Query para extrair os dados do Oracle
    query_pac = f"""SELECT DISTINCT
                    EMPRESA,
                    CONTA,
                    PERIODO,
                    VLR_TRANSACAO,
                    TIPO_ORIGEM,
                    DOCUMENTO_ORIGEM,
                    FONTE,
                    DATA_TRANSACAO,
                    ID_TRANSACAO,
                    CENTRO,
                    '' AS DATA_BASE
                FROM BOLINF.XXCST_CMV_V A
                WHERE DATA_TRANSACAO BETWEEN TO_DATE(:DT_INI, 'YYYY-MM-DD') AND TO_DATE(:DT_FIM, 'YYYY-MM-DD')
                and CONTA IN ({contas_formatadas})
                """

    # Inicialize as variáveis de contagem
    total_inseridos = 0
    total_deletados = 0

    try:
        # Converter as variáveis DT_INI e DT_FIM para o formato YYYY-MM-DD
        dt_ini = datetime.strptime(
            variaveis_de_configuracao['DT_INI'], '%d/%m/%Y').strftime('%Y-%m-%d')
        dt_fim = datetime.strptime(
            variaveis_de_configuracao['DT_FIM'], '%d/%m/%Y').strftime('%Y-%m-%d')

        # Extrair os dados do Oracle
        print(Fore.GREEN, end='')  # Define a cor verde para o texto
        with alive_bar(len(empresas), title=f"Extraindo dados do PAC") as bar:
            # Iterar sobre cada empresa
            for empresa in empresas:
                # Filtrar a query base por empresa
                select_query = query_pac + \
                    f" AND EMPRESA = '{empresa}'"

                base_pac = pd.read_sql_query(select_query, conn_TP215, params={
                                             'DT_INI': dt_ini, 'DT_FIM': dt_fim})
                bar()

                # Adiciona a coluna DATA_ATUALIZACAO com a data e hora atual
                base_pac['DATA_ATUALIZACAO'] = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S')

                # Verifica se a tabela existe
                table_exists = conn.execute(
                    """SELECT name FROM sqlite_master WHERE type='table' AND name='BASE_PAC';""").fetchone()

                if table_exists:

                    # Deleta da tabela BASE_PAC os registros que estão sendo importados
                    try:
                        deletados = conn.execute("""DELETE FROM BASE_PAC
                         WHERE DATA_TRANSACAO BETWEEN :DT_INI AND :DT_FIM
                         AND EMPRESA = :EMPRESA
                         """, {
                            'DT_INI': dt_ini,
                            'DT_FIM': dt_fim,
                            'EMPRESA': empresa
                        }).rowcount
                        conn.commit()

                        # Atualiza a contagem de deletados
                        total_deletados += deletados

                    except Exception as e:
                        print(f"{RED}Erro ao deletar registros: {e}")

                # Insere os dados do PAC no sqlite
                base_pac.to_sql('BASE_PAC', conn, if_exists='append', index=False, dtype={
                                'DATA_ATUALIZACAO': 'DATETIME'})

                # Atualiza a contagem de inseridos
                total_inseridos += len(base_pac)

                # Imprime mensagem de status
                end_time = time.time()  # Finaliza o temporizador
                elapsed_time = end_time - start_time
                print(
                    f"{GREEN}Consulta concluída para a empresa: {empresa} em {elapsed_time:.2f} segundos")

        # Imprime as contagens totais
        print(f"{GREEN}Total de registros deletados Base PAC: {total_deletados}")
        print(f"{GREEN}Total de registros inseridos Base PAC: {total_inseridos}")

    except Exception as e:
        print(f"{RED}Erro ao processar os dados: {e}")

# Update na BASE PAC
if variaveis_de_controle['update_base_pac']:
    # Atualizar a coluna DATA_BASE da PAC
    try:
        cursor = conn.cursor()

        # Obter o número total de registros onde DATA_BASE é nula
        cursor.execute("SELECT COUNT(*) FROM BASE_PAC WHERE DATA_BASE IS NULL")
        total_registros = cursor.fetchone()[0]

        # Definir o tamanho do lote
        batch_size = 10000
        num_batches = (total_registros // batch_size) + 1

        with alive_bar(num_batches, title="Atualizando DATA_BASE") as bar:
            for batch in range(num_batches):
                update_query = f"""
                UPDATE BASE_PAC
                SET DATA_BASE = '01/' || 
                    CASE 
                        WHEN substr(PERIODO, 1, 3) = 'JAN' THEN '01'
                        WHEN substr(PERIODO, 1, 3) = 'FEV' THEN '02'
                        WHEN substr(PERIODO, 1, 3) = 'MAR' THEN '03'
                        WHEN substr(PERIODO, 1, 3) = 'ABR' THEN '04'
                        WHEN substr(PERIODO, 1, 3) = 'MAI' THEN '05'
                        WHEN substr(PERIODO, 1, 3) = 'JUN' THEN '06'
                        WHEN substr(PERIODO, 1, 3) = 'JUL' THEN '07'
                        WHEN substr(PERIODO, 1, 3) = 'AGO' THEN '08'
                        WHEN substr(PERIODO, 1, 3) = 'SET' THEN '09'
                        WHEN substr(PERIODO, 1, 3) = 'OUT' THEN '10'
                        WHEN substr(PERIODO, 1, 3) = 'NOV' THEN '11'
                        WHEN substr(PERIODO, 1, 3) = 'DEZ' THEN '12'
                    END || 
                    '/' || '20' || substr(PERIODO, 5, 2)
                WHERE rowid IN (
                    SELECT rowid
                    FROM BASE_PAC
                    WHERE DATA_BASE IS NULL
                    LIMIT {batch_size} OFFSET {batch * batch_size}
                );
                """
                cursor.execute(update_query)
                conn.commit()
                bar()

        print(f"{Fore.GREEN}Coluna DATA_BASE atualizada com sucesso.")

    except sqlite3.Error as e:
        print(f"{Fore.RED}Erro ao atualizar a coluna DATA_BASE: {e}")

# Criação das tabelas de ajustes
if variaveis_de_controle['ajuste_manual_pac']:

    print(f"{BLUE}Importa Ajustes Manuais")
    # Caminho da pasta contendo os arquivos de depara
    pasta_ajustes_manuais = os.path.join(
        r'\\172.22.0.33',
        'Controladoria',
        '_Exercicio 2023',
        '34 - Somos',
        '3 - Bases Relatorio CPV',
        '4. Base Ajuste Manual'
    )

    try:
        # Lista todos os arquivos na pasta
        for arquivo_manuais in os.listdir(pasta_ajustes_manuais):
            # Verifica se o arquivo é um arquivo Excel (.xlsx)
            if arquivo_manuais.endswith('.xlsx'):
                caminho_completo_ajustes = os.path.join(
                    pasta_ajustes_manuais, arquivo_manuais)

                # Leia o arquivo Excel sem considerar nenhuma linha como cabeçalho e pula a primeira linha do arquivo
                depara = pd.read_excel(caminho_completo_ajustes)

                # Usa o nome do arquivo (sem a extensão) como o nome da tabela
                nome_tabela = f"{os.path.splitext(arquivo_manuais)[0]}_AJUSTE_MANUAL"

                # Insere os dados no banco de dados, substituindo a tabela se ela já existir
                depara.to_sql(nome_tabela, conn,
                              if_exists='replace', index=False)

                print(f"{GREEN}Tabela {nome_tabela} criada com sucesso.")
    except Exception as e:
        print(f"{RED}Erro ao processar os arquivos: {e}")

    try:
        cursor = conn.cursor()

        # Função para deletar registros da BASE_PAC que existem na tabela manual informada
        def deletar_registros(cursor, nome_tabela, empresa):
            delete_query = f"""
            DELETE FROM BASE_PAC 
            WHERE EMPRESA = '{empresa}'
            AND EXISTS (
                SELECT 1 FROM {nome_tabela} B
                WHERE 
                    BASE_PAC.EMPRESA = B.EMPRESA
                    AND BASE_PAC.PERIODO = B.PERIODO
                    AND BASE_PAC.CONTA = B.CONTA
                    AND BASE_PAC.FONTE = B.FONTE
                    AND BASE_PAC.ID_TRANSACAO = B.ID_TRANSACAO
                    AND BASE_PAC.VLR_TRANSACAO = B.VLR_TRANSACAO
            )
            """
            cursor.execute(delete_query)
            conn.commit()
            print(
                f"{GREEN}Registros deletados da tabela {nome_tabela} para a empresa {empresa} com sucesso.")

        # Função para inserir registros na BASE_PAC a partir da tabela manual informada
        def inserir_dados(cursor, nome_tabela):
            insert_query = f"""
            INSERT INTO BASE_PAC
            (EMPRESA, PERIODO, VLR_TRANSACAO, TIPO_ORIGEM, DOCUMENTO_ORIGEM, FONTE,
            DATA_TRANSACAO, ID_TRANSACAO, DATA_BASE, CENTRO, CONTA)
            SELECT EMPRESA, PERIODO, VLR_TRANSACAO, TIPO_ORIGEM, DOCUMENTO_ORIGEM, FONTE,
                DATA_TRANSACAO, ID_TRANSACAO, DATA_BASE, CENTRO, CONTA
            FROM {nome_tabela}
            """
            cursor.execute(insert_query)
            conn.commit()
            print(
                f"{GREEN}Registros da tabela {nome_tabela} inseridos na BASE_PAC com sucesso.")

        # Função para processar tabelas de uma empresa específica
        def processar_tabelas(cursor, empresa, tabelas):
            print(
                f"{WHITE}Aplicando Alterações Manuais das Bases Manuais PAC {empresa}")
            for tabela_manual in tabelas:
                deletar_registros(cursor, tabela_manual, empresa)
                inserir_dados(cursor, tabela_manual)
            print(f"{GREEN}Ajustes na PAC {empresa} aplicados com sucesso!")

        # Processar as tabelas para a empresa 35
        processar_tabelas(cursor, '35', ["BASE_ELABORA_35_AJUSTE_MANUAL",
                                         "BASE_ELABORA_35_2_AJUSTE_MANUAL", "BASE_PAC_35_AJUSTE_MANUAL"])

        # Processar as tabelas para a empresa 157
        processar_tabelas(cursor, '157', [
            "BASE_ELABORA_157_AJUSTE_MANUAL", '"Dif looping Maxi_AJUSTE_MANUAL"'])

    except Exception as e:
        conn.rollback()
        print(f"{RED}Erro ao aplicar ajustes: {e}")

    finally:
        # Fechar o cursor
        if cursor:
            cursor.close()

# sas
if variaveis_de_controle['importa_sas']:
    print(f"{BLUE}Importa Base SAS")

    try:
        # Caminho do arquivo .sas7bdat
        caminho_arquivo_sas = os.path.join(
            r'\\172.22.0.33',
            'Controladoria',
            '_Exercicio 2023',
            '34 - Somos',
            '3 - Bases Relatorio CPV',
            '1. Bases SAS',
            'base_pac_157.sas7bdat'
        )

        # Ler o arquivo .sas7bdat e transformar em um DataFrame
        with SAS7BDAT(caminho_arquivo_sas) as f:
            df_sas = f.to_data_frame()

            # Salva o df no sqlite
            df_sas.to_sql('BASE_PAC_139', conn,
                          if_exists='replace', index=False)

        print(f"{GREEN}Arquivo SAS importado com sucesso.")

    except Exception as e:
        print(f"{RED}Erro ao importar o arquivo SAS: {e}")

# Realizando os cálculos ~~ Novo Processo:  (MESMA QUE A BASE_CALCULLO_CPV DO SAS)
if variaveis_de_controle['calcula_cpv']:
    # Criação da tabela BASE_CALCULO_CPV
    print(f"{BLUE}Criando a tabela BASE_CALCULO_CPV")

    try:
        # Query para criar a tabela BASE_CALCULO_CPV COM OS DADOS DA BASE
        print(f"{BLUE}Salvando DF com a Base Grades")
        query_base_Calculo_CPV_Base_Grades = """
        SELECT DISTINCT
            strftime('%d%m%Y', DATA_BASE) AS DATA_BASE,
            A.EMPRESA,
            A.CONTA,
            'BASE_GRADES' AS FONTE,
            'Saldo Inicial' AS CALCULO,
            ROUND(SUM(SALDO_FINAL), 0.01) AS VALOR
        FROM BALANCETE_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE SALDO_FINAL <> 0
        AND B.GRUPO IN ('1.1 - MATERIAS PRIMAS Total', '1.3 - PRODUTOS EM ELABORACAO', '1.2 - PRODUTO ACABADO')
        AND FONTE <> 'BASE_RAZAO'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA
        """
        # Executar a query e armazenar os resultados do calculo cpv base grades
        df_Calculo_CPV_Base_Grades = pd.read_sql(
            query_base_Calculo_CPV_Base_Grades, conn)
        print(f"{GREEN}DF Calculo_Cpv_Base_Grades criada com sucesso!")
    except Exception as e:
        print(f"{RED}Erro ao criar DF Calculo_Cpv_Base_Grades: {e}")

    try:
        # Query para criar a tabela BASE_CALCULO_CPV COM OS DADOS DA RAZAO
        print(f"{BLUE}Salvando DF com a Base Razao")
        query_base_Calculo_CPV_Base_Razao = """
        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Material e serviços' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND A.CONTA IN (1140201004, 3210190001, 3210190002)
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION

        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Material e serviços' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND ORIGEM = 'Contas a Pagar'
        AND B.GRUPO IN ('1.1 - MATERIAS PRIMAS Total', '1.3 - PRODUTOS EM ELABORACAO', '1.2 - PRODUTO ACABADO')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION

        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'AVP' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'AVP'
        AND B.GRUPO IN ('1.1 - MATERIAS PRIMAS Total', '1.2 - PRODUTO ACABADO')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION

        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Outros' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'Reclassificações/Outros'
        AND A.CONTA NOT IN (1140310006, 1140311999, 1149901001, 1149901002, 1149901004)
        AND B.GRUPO IN ('1.1 - MATERIAS PRIMAS Total', '1.3 - PRODUTOS EM ELABORACAO', '1.2 - PRODUTO ACABADO')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION

        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Prov. Cut Off' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'Reclassificações/Outros'
        AND A.CONTA IN (1140310006)
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION

        SELECT 
            DATA_BASE,
            EMPRESA,
            CONTA,
            'BASE_RAZAO' AS FONTE,
            'Prov. IFRS 15' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        WHERE CLASSIFICACAO = 'Reclassificações/Outros'
        AND CONTA = 1149901004
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION

        SELECT 
            DATA_BASE,
            EMPRESA,
            CONTA,
            'BASE_RAZAO' AS FONTE,
            'Prov. Obsoletos' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        WHERE CLASSIFICACAO = 'Reclassificações/Outros'
        AND CONTA = 1149901001
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA
        """
        # Executar a query e armazenar os resultados do calculo cpv base grades
        df_Calculo_CPV_Base_Razao = pd.read_sql(
            query_base_Calculo_CPV_Base_Razao, conn)
        print(f"{GREEN}DF Calculo_Cpv_Base_Razao criada com sucesso!")
    except Exception as e:
        print(f"{RED}Erro ao criar a DF Calculo_Cpv_Base_Razap: {e}")

    try:
        # Query para criar a tabela BASE_CALCULO_CPV COM OS DADOS DA PAC
        print(f"{BLUE}Salvando DF com a Base PAC")

        # Obter a lista de empresas
        query_empresas = "SELECT DISTINCT EMPRESA FROM DEPARA_EMPRESAS"
        empresas = pd.read_sql(query_empresas, conn)['EMPRESA'].tolist()

        # Criar lista para armazenar os DataFrames do FOR
        df_list_pac_cpv = []

        for empresa in empresas:
            # Query para criar a tabela BASE_CALCULO_CPV COM OS DADOS DA PAC
            query_base_Calculo_CPV_Base_Pac = f"""
            SELECT DISTINCT
                {data_base_conversion},
                A.EMPRESA as 'EMPRESA',
                A.CONTA as 'CONTA',
                'BASE_PAC' AS FONTE,
                'Entradas de NFs' AS CALCULO,
                ROUND(SUM(vlr_transacao), 2) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND FONTE IN ('BASE COMPRAS', 'BASE COMPRAS APROPR')
            AND A.CONTA IN (1140101982, 1140101984)
            AND A.EMPRESA = '{empresa}'
            GROUP BY DATA_BASE, A.EMPRESA, A.CONTA

            UNION

            SELECT DISTINCT
                {data_base_conversion},
                A.EMPRESA as 'EMPRESA',
                A.CONTA as 'CONTA',
                'BASE_PAC' AS FONTE,
                'Entradas de NFs Serviços' AS CALCULO,
                ROUND(SUM(vlr_transacao), 2) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND FONTE = 'BASE ELAB'
            AND A.EMPRESA = '{empresa}'
            AND B.GRUPO IN ('1.3 - PRODUTOS EM ELABORACAO')
            GROUP BY DATA_BASE, A.EMPRESA, A.CONTA

            UNION

            SELECT DISTINCT
                {data_base_conversion},
                A.EMPRESA as 'EMPRESA',
                A.CONTA as 'CONTA',
                'BASE_PAC' AS FONTE,
                'Entrada Custo Folha MO' AS CALCULO,
                ROUND(SUM(vlr_transacao), 2) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND FONTE IN ('BASE COMPRAS', 'BASE COMPRAS APROPR')
            AND A.EMPRESA = '{empresa}'
            AND A.CONTA IN (1140201004)
            GROUP BY DATA_BASE, A.EMPRESA, A.CONTA
            """

            # Executar a query
            df_empresa_pac = pd.read_sql(query_base_Calculo_CPV_Base_Pac, conn)
            # Adiciona os resultados na lista
            df_list_pac_cpv.append(df_empresa_pac)
            print(
                f"{GREEN}Dados PAC da empresa {empresa} inseridos com sucesso no DF LIST PAC CPV")

        # Filtrar DataFrames vazios antes de concatenar
        df_list_pac_cpv = [df for df in df_list_pac_cpv if not df.empty]

        # Armazenando o for da PAC em um único DataFrame
        if df_list_pac_cpv:
            df_Calculo_CPV_Base_PAC = pd.concat(
                df_list_pac_cpv, ignore_index=True)
            print(f"{Fore.GREEN}DF CALCULO_CPV_BASE_PAC criada com sucesso!")
        else:
            df_Calculo_CPV_Base_PAC = pd.DataFrame()
            print(
                f"{Fore.RED}Nenhum dado encontrado para criar DF CALCULO_CPV_BASE_PAC.")

    except Exception as e:
        print(f"{RED}Erro ao inserir dados na tabela BASE_CALCULO_CPV: {e}")

    try:
        # Unindo as três consultas
        df_final = pd.concat([df_Calculo_CPV_Base_Grades, df_Calculo_CPV_Base_Razao,
                              df_Calculo_CPV_Base_PAC], ignore_index=True)

        # Substituir a tabela final BASE_CALCULO_CPV
        df_final.to_sql("BASE_CALCULO_CPV", conn,
                        if_exists="replace", index=False)
        print(f"{Fore.GREEN}Tabela BASE_CALCULO_CPV atualizada com sucesso!")

    except Exception as e:
        print(f"{Fore.RED}Erro ao substituir a tabela BASE_CALCULO_CPV: {e}")

        # Extraindo base CALCULO CPV
        try:
            df_base_calculo_cpv = pd.read_sql_query(
                "SELECT * FROM BASE_CALCULO_CPV", conn)
            caminho_arquivo_xlsx = os.path.join(
                pasta_banco_dados, 'BASE_CALCULO_CPV.xlsx')
            df_base_calculo_cpv.to_excel(caminho_arquivo_xlsx, index=False)
            print(
                f"{GREEN}Tabela BASE_CALCULO_CPV exportada com sucesso para {caminho_arquivo_xlsx}")
        except Exception as e:
            print(f"{RED}Erro ao exportar a tabela BASE_CALCULO_CPV: {e}")

# CRIA TABELA PAC POR EMPRESA
if variaveis_de_controle['calcula_compras_pac']:
    print(f"{BLUE}Criando tabelas BASE_COMPRAS_CPV para cada empresa")

    try:
        # Obter a lista de empresas
        query_empresas = "SELECT DISTINCT EMPRESA FROM DEPARA_EMPRESAS"
        empresas = pd.read_sql(query_empresas, conn)['EMPRESA'].tolist()

        for empresa in empresas:
            # Criar a tabela BASE_COMPRAS_CPV para a empresa atual
            query_base_calculo_compras_cpv = f"""
            SELECT DISTINCT
                {data_base_conversion},
                A.EMPRESA,
                A.CONTA,
                'BASE_PAC' AS FONTE,
                'Entradas de NFs' AS COLUNA,
                ROUND(SUM(vlr_transacao), 0.01) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND FONTE IN ('BASE COMPRAS', 'BASE COMPRAS APROPR')
            AND A.CONTA IN (1140101982, 1140101984)
            AND A.EMPRESA = '{empresa}'
            GROUP BY DATA_BASE, A.EMPRESA, A.CONTA

            UNION
            
            SELECT DISTINCT
               {data_base_conversion},
                A.EMPRESA,
                A.CONTA,
                'BASE_PAC' AS FONTE,
                'Entradas de NFs Serviços' AS COLUNA,
                ROUND(SUM(vlr_transacao), 0.01) AS VALOR
            FROM BASE_PAC  A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND FONTE = 'BASE ELAB'
            AND A.EMPRESA = '{empresa}'
            AND B.GRUPO IN ('1.3 - PRODUTOS EM ELABORACAO')
            GROUP BY DATA_BASE, A.EMPRESA, A.CONTA

            UNION

            SELECT DISTINCT
                {data_base_conversion},
                A.EMPRESA,
                A.CONTA,
                'BASE_PAC' AS FONTE,
                'Entrada Custo Folha MO' AS COLUNA,
                ROUND(SUM(vlr_transacao), 0.01) AS VALOR
            FROM BASE_PAC  A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND FONTE IN ('BASE COMPRAS', 'BASE COMPRAS APROPR')
            AND A.EMPRESA = '{empresa}'
            AND A.CONTA IN (1140201004)
            GROUP BY DATA_BASE, A.EMPRESA, A.CONTA
            """
            # Executar a query e armazenar os resultados em um DataFrame
            df_pac_empresas = pd.read_sql(query_base_calculo_compras_cpv, conn)

            # Nome da tabela específica para a empresa
            table_name = f"BASE_PAC_{empresa}"

            # Criar ou atualizar a tabela específica para a empresa
            df_pac_empresas.to_sql(
                table_name, conn, if_exists="replace", index=False)

            print(f"{GREEN}Tabela {table_name} atualizada com sucesso!")
    except Exception as e:
        print(f"{RED}Erro ao criar as tabelas BASE_PAC_EMPRESA: {e}")

# CRIA TABELA DE COMPRAS CPV
if variaveis_de_controle['calcula_compras_cpv']:
    print(f"{BLUE}Criando a tabela CALCULO_COMPRAS_CPV")

    try:
        # Antes de inserir os dados, exclua a tabela se ela existir
        conn.execute("DROP TABLE IF EXISTS CALCULO_COMPRAS_CPV;")

        # Executa a query e salva os resultados em um DataFrame
        query_calculo_compras_cpv = """
            SELECT DISTINCT
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Entradas de NFs' AS CALCULO,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND ORIGEM = 'Contas a Pagar'
        AND A.CONTA IN (1140101982, 1140101984)
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA
        UNION
          SELECT DISTINCT
            DATA_BASE,
            EMPRESA,
            CONTA,
            'BASE_PAC/RAZAO' AS FONTE,
            'Composição das compras' AS CALCULO,
            SUM(VALOR) AS VALOR
        FROM BASE_CALCULO_CPV
        WHERE CALCULO IN ('Compras Material e serviços', 'Serviços elaboração', 'AVP')
        GROUP BY DATA_BASE, EMPRESA, CONTA
        UNION
        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Entrada Custo Folha MO' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND A.CONTA IN (1140201004)
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA
        UNION
        SELECT DISTINCT
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Lançamentos de AVP' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'AVP'
        AND B.GRUPO IN ('1.2 - PRODUTO ACABADO', '1.1 - MATERIAS PRIMAS Total')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA
        UNION
        SELECT 
            DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Outros Lançamentos' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 0.01) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'Reclassificações/Outros'
        AND A.CONTA = 1140201004
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA
        UNION
         SELECT DISTINCT
            DATA_BASE,
            EMPRESA,
            CONTA,
            'BASE_PAC/RAZAO' AS FONTE,
            'Check' AS CALCULO,
            1 AS VALOR
        FROM BASE_CALCULO_CPV
        WHERE CALCULO IN ('Compras Material e serviços', 'Serviços elaboração', 'AVP')
        GROUP BY DATA_BASE, EMPRESA, CONTA
        """

        # Salva a query em um DataFrame
        df_compras_cpv = pd.read_sql(query_calculo_compras_cpv, conn)

        # Substitui a tabela no banco de dados com os novos dados
        df_compras_cpv.to_sql("CALCULO_COMPRAS_CPV", conn,
                              if_exists="replace", index=False)

        print(f"{GREEN}Tabela CALCULO_COMPRAS_CPV criada/atualizada com sucesso!")

    except Exception as e:
        print(f"{RED}Erro ao calcular Compras CPV: {e}")

# CRIA BASE DEMONSTRATIVA CPV
if variaveis_de_controle['Primeiro_Demonstrativo_CPV']:
    print(f"{Fore.BLUE}Gerando 1° Demonstrativo CPV")

    try:
        # Query para criar a tabela BASE_CALCULO_CPV COM OS DADOS DA BASE GRADES
        query_base_demonstrativo_cpv = """
            SELECT DISTINCT
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA AS EMPRESA,
            A.CONTA,
            'BASE_GRADES' AS FONTE,
            'Saldo Inicial' AS COLUNA,
            ROUND(SUM(SALDO_FINAL), 2) AS VALOR
        FROM BALANCETE_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE SALDO_FINAL <> 0
        AND strftime('%m', DATA_BASE) = '12'
        AND B.GRUPO IN ('1.1 - MATERIAS PRIMAS Total', '1.3 - PRODUTOS EM ELABORACAO', '1.2 - PRODUTO ACABADO')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT DISTINCT
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA AS EMPRESA,
            A.CONTA,
            'BASE_GRADES' AS FONTE,
            'Saldo Inicial' AS COLUNA,
            ROUND(SUM(SALDO_FINAL), 2) AS VALOR
        FROM BALANCETE_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE SALDO_FINAL <> 0
        AND strftime('%m', DATA_BASE) <> '12'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL
        
        SELECT DISTINCT
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA AS EMPRESA,
            A.CONTA,
            'BASE_GRADES' AS FONTE,
            'Saldo Grade' AS COLUNA,
            ROUND(SUM(SALDO_FINAL), 2) AS VALOR
        FROM BALANCETE_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE SALDO_FINAL <> 0
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL
        

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Razão' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND A.CONTA IN (1140201004, 3210190001, 3210190002)
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Razão' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND ORIGEM = 'Contas a Pagar'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Razão' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'COMPRAS'
        AND REPLACE(ORIGEM, ' ', '') = REPLACE('CLL F189 INTEGRATED RCV', ' ', '')
        AND A.EMPRESA = '139'
        AND A.CONTA <> 3210180002
        AND REPLACE(B.GRUPO, ' ', '') = REPLACE('3.1 - CUSTO SOBRE VENDAS TRANSITORIAS 321', ' ', '')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'AVP' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'AVP'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Custo Serviços / Outros' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'Reclassificações/Outros'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Custo Serviços / Outros' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO = 'PAC'
        AND LANCAMENTO = 'PAC - VARIACAO BRL'
        AND A.CONTA IN (1140201004)
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Razão' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO IN ('SERVIÇOS')
        AND REPLACE(LANCAMENTO, ' ', '') IN (
        REPLACE('72158174 NFFs de Compra BRL', ' ', ''), 
        REPLACE('72158175 NFFs de Compra BRL', ' ', ''), 
        REPLACE('72186532 NFFs de Compra BRL', ' ', ''), 
        REPLACE('72668678 NFFs de Compra BRL', ' ', ''), 
        REPLACE('72487479 NFFs de Compra BRL', ' ', ''))
        AND A.CONTA IN (1140101982)
        AND A.EMPRESA = '139'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Razão' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE CLASSIFICACAO IN ('PAC')
        AND REPLACE(HISTORICO, ' ', '') = REPLACE('819901-ITEM DOC 877-GLOBAL TECH RESOURCES LTDA  EPP; 243070-SENSOR DE TEMPERATURA I12', ' ', '')
        AND A.CONTA IN (1140101982)
        AND EMPRESA = '139'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Compras Razão' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE A.CONTA IN (3210180993)
        AND EMPRESA = '139'
        AND CLASSIFICACAO IN ('PAC', 'INV')
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA

        UNION ALL

        SELECT 
            strftime('%d/%m/%Y', DATA_BASE) AS DATA_BASE,
            EMPRESA,
            A.CONTA,
            'BASE_RAZAO' AS FONTE,
            'Custo Serviços / Outros' AS COLUNA,
            ROUND(SUM(VLR_MOEDA_NACIONAL), 2) AS VALOR
        FROM RAZAO_EBS A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE HISTORICO LIKE '%TRANSF.P/ELAB. - Editorial Intercompany - CL_%'
        AND A.CONTA IN (3210180995)
        AND EMPRESA = '139'
        GROUP BY A.DATA_BASE, A.EMPRESA, A.CONTA;
        """
        # Executar a query e armazenar os resultados em um DataFrame
        df = pd.read_sql(query_base_demonstrativo_cpv, conn)

        # Criar ou atualizar a tabela
        df.to_sql("DEMONSTRATIVO_CPV", conn, if_exists="replace", index=False)

        print(f"{Fore.GREEN}Tabela DEMONSTRATIVO_CPV atualizada com sucesso!")

        # # Deletar registros onde FONTE está em ('Grades/Pac/Razão')
        # delete_query = """
        # DELETE FROM DEMONSTRATIVO_CPV WHERE FONTE IN ('BASE_GRADES', 'BASE_PAC', 'BASE_RAZAO')
        # """
        # conn.execute(delete_query)
        # conn.commit()

        # print(f"{Fore.GREEN}Registros deletados com sucesso!")
    except Exception as e:
        print(f"{Fore.RED}Erro ao criar a tabela DEMONSTRATIVO_CPV: {e}")

        # CRIA BASE SUMARIZADA DO DEMONSTRATIVO CPV

# CRIA BASE SUMARIZADA DO DEMONSTRATIVO CPV
if variaveis_de_controle['Segundo_Demonstrativo_CPV']:
    print(f"{Fore.BLUE}Gerando 2° Demonstrativo CPV")

    try:
        # Passo 1:Base Demontrativo CPV_2, FONTE: RBS+BALANCETE
        query_base_sumarizada_demonstrativo_cpv = """
        SELECT DISTINCT
        DATA_BASE,
        EMPRESA,
        CONTA,
        FONTE,
        COLUNA,
        VALOR
        FROM DEMONSTRATIVO_CPV
        UNION ALL
        SELECT DISTINCT
        DATA_BASE,
        EMPRESA,
        CONTA,
        'GRADES/PAC/RAZÃO' AS FONTE,
        'Total' as COLUNA,
        SUM(VALOR) AS VALOR
        FROM DEMONSTRATIVO_CPV
        WHERE COLUNA <> 'Saldo Grade'
        GROUP BY DATA_BASE, EMPRESA, CONTA   
        
        UNION ALL

        SELECT DISTINCT
        DATA_BASE,
        EMPRESA,
        CONTA,
        'BASE_GRADES' AS FONTE,
        'Fluxo' AS COLUNA,
        1 AS VALOR
        FROM DEMONSTRATIVO_CPV
        WHERE COLUNA IN ('Saldo Grade', 'Saldo Inicial')
        GROUP BY DATA_BASE, EMPRESA, CONTA

        UNION ALL

        SELECT DISTINCT
        DATA_BASE,
        EMPRESA,
        CONTA,
        'Grades/Pac/Razão' AS FONTE,
        'Diferença' AS COLUNA,
        1 AS VALOR
        FROM DEMONSTRATIVO_CPV
        GROUP BY DATA_BASE, EMPRESA, CONTA

        UNION ALL

        SELECT DISTINCT
        DATA_BASE,
        EMPRESA,
        0 AS CONTA,
        FONTE,
        COLUNA,
        VALOR
        FROM DEMONSTRATIVO_CPV A
        INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
        WHERE B.GRUPO IN ('3.1 - CUSTO SOBRE VENDAS', '3.2 - CUSTO SOBRE VENDAS');
        """

        df1 = pd.read_sql(query_base_sumarizada_demonstrativo_cpv, conn)
        print(f"{Fore.GREEN}Primeira query executada BALANCETE+RBS")

    except Exception as e:
        print(f"{Fore.RED}Erro ao executar a primeira query BALANCETE+RBS: {e}")

    try:
        # Passo 2: Obter a lista de empresas
        query_empresas = "SELECT DISTINCT EMPRESA FROM DEPARA_EMPRESAS"
        empresas = pd.read_sql(query_empresas, conn)['EMPRESA'].tolist()

        # Criar lista para armazenar os DataFrames da segunda query
        df_list = []

        for empresa in empresas:
            query_base_sumarizada_demonstrativo_cpv_PAC = f"""
            SELECT DISTINCT
                A.DATA_BASE, 
                A.EMPRESA AS EMPRESA,
                A.CONTA AS 'CONTA',
                'BASE_PAC' AS FONTE,
                'Base PAC' AS COLUNA,
                ROUND(SUM(vlr_transacao), 2) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0	
            AND FONTE IN ('BASE PAC')
            AND A.EMPRESA = '{empresa}'
            AND VLR_TRANSACAO <> 0
            GROUP BY A.EMPRESA, A.CONTA

            UNION ALL  

            SELECT DISTINCT
                A.DATA_BASE,
                A.EMPRESA AS EMPRESA,
                A.CONTA as CONTA,
                'BASE_PAC' AS FONTE,
                'Base Elaboração' AS COLUNA,
                ROUND(SUM(vlr_transacao), 2) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND A.EMPRESA = '{empresa}'
            AND FONTE = 'BASE ELAB'
            AND NOT (A.CONTA = 3210180993 AND A.EMPRESA = '139')
            GROUP BY A.EMPRESA, A.CONTA

            UNION ALL

            SELECT DISTINCT
                A.DATA_BASE,
                A.EMPRESA as EMPRESA,
                A.CONTA as CONTA,
                'BASE_PAC' AS FONTE,
                'Compras PAC' AS COLUNA,
                ROUND(SUM(vlr_transacao), 2) AS VALOR
            FROM BASE_PAC A
            INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
            WHERE vlr_transacao <> 0
            AND A.EMPRESA = '{empresa}'
            AND A.EMPRESA <> '139'
            AND (
                A.EMPRESA <> '139' 
                OR (A.EMPRESA = '139' AND (B.GRUPO <> '3.1 - CUSTO SOBRE VENDAS TRANSITORIAS 321' OR A.CONTA = '3210180002'))
            )
            GROUP BY A.EMPRESA, A.CONTA;
            """
            # Executar a query
            df_empresa = pd.read_sql(
                query_base_sumarizada_demonstrativo_cpv_PAC, conn)
            # Adiciona os resultados na lista
            df_list.append(df_empresa)  # Adiciona os resultados na lista
            print(
                f"{GREEN}Dados PAC da empresa {empresa} inseridos com sucesso no DF DEMONSTRATIVO LIST PAC CPV")

        # Filtrar DataFrames vazios antes de concatenar
        df_list = [df for df in df_list if not df.empty]

        # Armazenando o for da PAC em um único DataFrame
        if df_list:
            df2 = pd.concat(
                df_list, ignore_index=True)
            print(f"{Fore.GREEN}DF DEMONSTRATIVO PAC criada com sucesso!")
        else:
            df2 = pd.DataFrame()
            print(
                f"{Fore.RED}Nenhum dado encontrado para criar DF DEMONSTRATIVO.")

    except Exception as e:
        print(f"{RED}Erro ao inserir dados no DF do PAC: {e}")

    try:
        # Unindo as duas consultas
        df_final_demonstrativo = pd.concat([df1, df2], ignore_index=True)

        # Substituir a tabela final BASE_DEMONSTRATIVO
        df_final_demonstrativo.to_sql("DEMONSTRATIVO_CPV2", conn,
                                      if_exists="replace", index=False)
        print(f"{Fore.GREEN}Tabela DEMONSTRATIVO_CPV2 atualizada com sucesso!")

    except Exception as e:
        print(f"{Fore.RED}Erro ao substituir a tabela DEMONSTRATIVO_CPV2: {e}")

    # Extraindo bas DEMONSTRATIVO_CPV
    try:
        df_base_calculo_cpv = pd.read_sql_query(
            "SELECT * FROM DEMONSTRATIVO_CPV2", conn)
        caminho_arquivo_xlsx = os.path.join(
            pasta_banco_dados, 'DEMONSTRATIVO_CPV2.xlsx')
        df_base_calculo_cpv.to_excel(caminho_arquivo_xlsx, index=False)
        print(
            f"{GREEN}Tabela DEMONSTRATIVO_CPV2 exportada com sucesso para {caminho_arquivo_xlsx}")
    except Exception as e:
        print(f"{RED}Erro ao exportar a tabela DEMONSTRATIVO_CPV2: {e}")

# Exporta base analítica PAC
if variaveis_de_controle['exporta_base_analitica_PAC']:
    print(f"{Fore.BLUE}Atualizando BASE_ANALITICA_PAC_POR_EMPRESA")
    try:
        # Armazenando filtros para extração
        dt_ini_para_extracao = variaveis_para_extracao['DT_INI_PARA_EXTRACAO']
        dt_fim_para_extracao = variaveis_para_extracao['DT_FIM_PARA_EXTRACAO']
        empresas_para_extracao = variaveis_para_extracao['EMPRESA_PARA_EXTRACAO']

        # Caminho para extrair arquivos pac
        arquivos_analitico_pac = os.path.join(
            os.path.expanduser('~'),
            'OneDrive - EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
            '02- CONTROLADORIA',
            'RA - CPV',
            'PAC')

        # Adicionar a barra de progresso
        with alive_bar(len(empresas_para_extracao)) as bar:
            for empresa in empresas_para_extracao:
                try:
                    # Query para obter os dados
                    query_base_analitica_pac_empresa = f""" 
                    SELECT A.*,
                        B.GRUPO,
                        CASE 
                            WHEN FONTE = 'BASE PAC' THEN 'Base PAC'
                            WHEN FONTE = 'BASE ELAB' THEN 'Base Elaboração'
                            WHEN FONTE IN ('BASE COMPRAS', 'BASE COMPRAS APROPR') THEN 'Compras'
                        END AS COLUNA_CPV   
                    FROM BASE_PAC A
                    INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
                    WHERE A.VLR_TRANSACAO <> 0
                    AND A.EMPRESA = '{empresa}'
                    AND DATE(DATA_TRANSACAO) BETWEEN '{dt_ini_para_extracao}' AND '{dt_fim_para_extracao}'
                    AND FONTE IN ('BASE PAC', 'BASE ELAB', 'BASE COMPRAS', 'BASE COMPRAS APROPR') 
                    """

                    # Ler os dados em um DataFrame
                    df_analitica_pac_empresa = pd.read_sql(
                        query_base_analitica_pac_empresa, conn)

                    # Adicionar a coluna de ID sequencial
                    df_analitica_pac_empresa['ID'] = pd.Series(
                        range(1, len(df_analitica_pac_empresa) + 1))

                    # Verificar se a coluna PERIODO existe
                    if 'periodo' in df_analitica_pac_empresa.columns:
                        periodos_distintos = df_analitica_pac_empresa['periodo'].unique(
                        )

                        for periodo in periodos_distintos:
                            df_periodo = df_analitica_pac_empresa[df_analitica_pac_empresa['periodo'] == periodo]

                            # Dividir o DataFrame em partes menores
                            num_chunks = (len(df_periodo) // 1_000_000) + 1

                            for i in range(num_chunks):
                                inicio = i * 1_000_000
                                fim = inicio + 1_000_000
                                # <-- ADICIONADO .copy() PARA EVITAR WARNINGS
                                df_chunk = df_periodo.iloc[inicio:fim].copy()

                                if not df_chunk.empty:
                                    # Criar nome do arquivo com numeração (_1, _2, etc.)
                                    file_name = f"BASE_ANALITICA_PAC_{empresa}_{periodo}_{i+1}.xlsx"
                                    file_path = os.path.join(
                                        arquivos_analitico_pac, file_name)

                                    # Salvar cada pedaço no Excel
                                    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                                        df_chunk.to_excel(
                                            writer, sheet_name='Dados', index=False)

                                    print(
                                        f"{Fore.GREEN}Arquivo {file_name} salvo com sucesso!")
                    else:
                        print(
                            f"{Fore.RED}A coluna PERIODO não existe na consulta para a empresa {empresa}.")
                except Exception as e:
                    print(
                        f"{Fore.RED}Erro ao exportar a BASE_ANALITICA_PAC_{empresa}: {e}")
                bar()
    except Exception as e:
        print(f"{Fore.RED}Erro ao executar a atualização: {e}")

# Exporta base analitica GRADES
if variaveis_de_controle['exporta_base_analitica_GRADES']:
    print(f"{Fore.BLUE}Atualizando BASE_ANALITICA_GRADES")
    try:

        # Armazenando filtros para extração
        dt_ini_para_extracao = variaveis_para_extracao['DT_INI_PARA_EXTRACAO']
        dt_fim_para_extracao = variaveis_para_extracao['DT_FIM_PARA_EXTRACAO']
        empresas_para_extracao = variaveis_para_extracao['EMPRESA_PARA_EXTRACAO']
        print(
            f"Período de extração: {dt_ini_para_extracao} a {dt_fim_para_extracao}")
        print(f"Empresas para extração: {empresas_para_extracao}")

        # Caminho para extrair arquivos GRADES
        arquivos_analitico_GRADES = os.path.join(
            os.path.expanduser('~'),
            'OneDrive - EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
            '02- CONTROLADORIA',
            'RA - CPV',
            'GRADES')
        print(f"Caminho para salvar arquivos: {arquivos_analitico_GRADES}")

        # Adicionar a barra de progresso
        with alive_bar(len(empresas_para_extracao)) as bar:
            for empresa in empresas_para_extracao:
                try:
                    print(f"Processando empresa: {empresa}")
                    # Cria a tabela BASE_ANALITICA_GRADES_EMPRESAS
                    query_base_analitica_grades = f"""
                    SELECT A.*, B.GRUPO, 'BASE_GRADES' AS FONTE, 'SALDO GRADE' AS COLUNA_CPV,
                    CASE
                        WHEN substr(Período, 6, 2) = '01' THEN 'JAN'
                        WHEN substr(Período, 6, 2) = '02' THEN 'FEV'
                        WHEN substr(Período, 6, 2) = '03' THEN 'MAR'
                        WHEN substr(Período, 6, 2) = '04' THEN 'ABR'
                        WHEN substr(Período, 6, 2) = '05' THEN 'MAI'
                        WHEN substr(Período, 6, 2) = '06' THEN 'JUN'
                        WHEN substr(Período, 6, 2) = '07' THEN 'JUL'
                        WHEN substr(Período, 6, 2) = '08' THEN 'AGO'
                        WHEN substr(Período, 6, 2) = '09' THEN 'SET'
                        WHEN substr(Período, 6, 2) = '10' THEN 'OUT'
                        WHEN substr(Período, 6, 2) = '11' THEN 'NOV'
                        WHEN substr(Período, 6, 2) = '12' THEN 'DEZ'
                    END || '-' || substr(Período, 3, 2) AS PERIODO_FORMATADO
                    FROM BALANCETE_EBS A
                    INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
                    WHERE SALDO_FINAL <> 0
                    AND A.EMPRESA = '{empresa}'
                    AND DATE(Período) BETWEEN '{dt_ini_para_extracao}' AND '{dt_fim_para_extracao}'
                    """
                    print(f"Executando query para empresa {empresa}")
                    # Executar a query e armazenar os resultados da base analítica grades
                    df_analitica_grades_empresa = pd.read_sql(
                        query_base_analitica_grades, conn)
                    print(
                        f"Query executada com sucesso para empresa {empresa}")

                    # Adicionar a coluna de ID sequencial
                    df_analitica_grades_empresa['ID'] = pd.Series(
                        range(1, len(df_analitica_grades_empresa) + 1))

                    # Verificar se a coluna Período existe
                    if 'PERIODO_FORMATADO' in df_analitica_grades_empresa.columns:
                        # Obter os períodos distintos
                        periodos_distintos = df_analitica_grades_empresa['PERIODO_FORMATADO'].unique(
                        )
                        print(
                            f"Períodos distintos encontrados para empresa {empresa}: {periodos_distintos}")

                        for periodo in periodos_distintos:
                            # Filtrar o DataFrame para o período atual
                            df_periodo = df_analitica_grades_empresa[
                                df_analitica_grades_empresa['PERIODO_FORMATADO'] == periodo]

                            # Definir o nome do arquivo
                            file_name = f"BASE_ANALITICA_GRADES_{empresa}_{periodo}.xlsx"
                            file_path = os.path.join(
                                arquivos_analitico_GRADES, file_name)

                            # Salvar o DataFrame em um arquivo Excel
                            df_periodo.to_excel(file_path, index=False)
                            print(
                                f"{Fore.GREEN}Arquivo {file_name} salvo com sucesso!")
                    else:
                        print(
                            f"{Fore.RED}A coluna PERIODO não existe na consulta para a empresa {empresa}.")
                except Exception as e:
                    print(
                        f"{Fore.RED}Erro ao exportar a BASE_ANALITICA_GRADES_{empresa}: {e}")
                # Atualizar a barra de progresso
                bar()
    except Exception as e:
        print(f"{Fore.RED}Erro ao executar a atualização: {e}")

# Exporta base analiica RAZAO
if variaveis_de_controle['exporta_base_analitica_RAZAO']:
    print(f"{Fore.BLUE}Atualizando BASE_ANALITICA_RAZAO")
    try:
        # Pegando lista de Empresas
        query_empresas = "SELECT DISTINCT EMPRESA FROM DEPARA_EMPRESAS"
        empresas = pd.read_sql(query_empresas, conn)['EMPRESA'].tolist()
        print(f"Empresas encontradas: {empresas}")

        # Armazenando filtros para extração
        dt_ini_para_extracao = variaveis_para_extracao['DT_INI_PARA_EXTRACAO']
        dt_fim_para_extracao = variaveis_para_extracao['DT_FIM_PARA_EXTRACAO']
        empresas_para_extracao = variaveis_para_extracao['EMPRESA_PARA_EXTRACAO']
        print(
            f"Período de extração: {dt_ini_para_extracao} a {dt_fim_para_extracao}")
        print(f"Empresas para extração: {empresas_para_extracao}")

        # Caminho para extrair arquivos RAZAO
        arquivos_analitico_razao = os.path.join(
            os.path.expanduser('~'),
            'OneDrive - EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
            '02- CONTROLADORIA',
            'RA - CPV',
            'RAZAO')
        print(f"Caminho para salvar arquivos: {arquivos_analitico_razao}")

        # Adicionar a barra de progresso
        with alive_bar(len(empresas_para_extracao)) as bar:
            for empresa in empresas_para_extracao:
                try:
                    print(f"Processando empresa: {empresa}")
                    # Cria a tabela BASE_ANALITICA_RAZAO_EMPRESAS
                    query_base_analitica_razao = f"""
                    SELECT A.*, B.GRUPO, 'BASE_RAZAO' AS FONTE, A.CLASSIFICACAO AS "COLUNA_CPV",
                    CASE
                        WHEN substr(DATA_BASE, 6, 2) = '01' THEN 'JAN'
                        WHEN substr(DATA_BASE, 6, 2) = '02' THEN 'FEV'
                        WHEN substr(DATA_BASE, 6, 2) = '03' THEN 'MAR'
                        WHEN substr(DATA_BASE, 6, 2) = '04' THEN 'ABR'
                        WHEN substr(DATA_BASE, 6, 2) = '05' THEN 'MAI'
                        WHEN substr(DATA_BASE, 6, 2) = '06' THEN 'JUN'
                        WHEN substr(DATA_BASE, 6, 2) = '07' THEN 'JUL'
                        WHEN substr(DATA_BASE, 6, 2) = '08' THEN 'AGO'
                        WHEN substr(DATA_BASE, 6, 2) = '09' THEN 'SET'
                        WHEN substr(DATA_BASE, 6, 2) = '10' THEN 'OUT'
                        WHEN substr(DATA_BASE, 6, 2) = '11' THEN 'NOV'
                        WHEN substr(DATA_BASE, 6, 2) = '12' THEN 'DEZ'
                    END || '-' || substr(DATA_BASE, 3, 2) AS PERIODO_FORMATADO
                    FROM RAZAO_EBS A
                    INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
                    WHERE A.CLASSIFICACAO  IN ('Reclassificações/Outros')
                    AND A.EMPRESA = '{empresa}'
                    AND DATE(DATA_BASE) BETWEEN '{dt_ini_para_extracao}' AND  '{dt_fim_para_extracao}'
                    UNION
                    SELECT A.*, B.GRUPO, 'BASE_RAZAO' AS FONTE, A.CLASSIFICACAO AS "COLUNA_CPV",
                                        CASE
                        WHEN substr(DATA_BASE, 6, 2) = '01' THEN 'JAN'
                        WHEN substr(DATA_BASE, 6, 2) = '02' THEN 'FEV'
                        WHEN substr(DATA_BASE, 6, 2) = '03' THEN 'MAR'
                        WHEN substr(DATA_BASE, 6, 2) = '04' THEN 'ABR'
                        WHEN substr(DATA_BASE, 6, 2) = '05' THEN 'MAI'
                        WHEN substr(DATA_BASE, 6, 2) = '06' THEN 'JUN'
                        WHEN substr(DATA_BASE, 6, 2) = '07' THEN 'JUL'
                        WHEN substr(DATA_BASE, 6, 2) = '08' THEN 'AGO'
                        WHEN substr(DATA_BASE, 6, 2) = '09' THEN 'SET'
                        WHEN substr(DATA_BASE, 6, 2) = '10' THEN 'OUT'
                        WHEN substr(DATA_BASE, 6, 2) = '11' THEN 'NOV'
                        WHEN substr(DATA_BASE, 6, 2) = '12' THEN 'DEZ'
                    END || '-' || substr(DATA_BASE, 3, 2) AS PERIODO_FORMATADO
                    FROM RAZAO_EBS A
                    INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
                    WHERE A.CLASSIFICACAO  IN ('PAC')
                    AND LANCAMENTO = 'PAC - VARIACAO BRL'
                    AND A.EMPRESA = '{empresa}'
                    AND A.CONTA IN (1140201004)
                    AND DATE(DATA_BASE) BETWEEN '{dt_ini_para_extracao}' AND  '{dt_fim_para_extracao}'
                    UNION
                    SELECT A.*, B.GRUPO, 'BASE_RAZAO' AS FONTE, A.CLASSIFICACAO AS "COLUNA_CPV",
                    CASE
                        WHEN substr(DATA_BASE, 6, 2) = '01' THEN 'JAN'
                        WHEN substr(DATA_BASE, 6, 2) = '02' THEN 'FEV'
                        WHEN substr(DATA_BASE, 6, 2) = '03' THEN 'MAR'
                        WHEN substr(DATA_BASE, 6, 2) = '04' THEN 'ABR'
                        WHEN substr(DATA_BASE, 6, 2) = '05' THEN 'MAI'
                        WHEN substr(DATA_BASE, 6, 2) = '06' THEN 'JUN'
                        WHEN substr(DATA_BASE, 6, 2) = '07' THEN 'JUL'
                        WHEN substr(DATA_BASE, 6, 2) = '08' THEN 'AGO'
                        WHEN substr(DATA_BASE, 6, 2) = '09' THEN 'SET'
                        WHEN substr(DATA_BASE, 6, 2) = '10' THEN 'OUT'
                        WHEN substr(DATA_BASE, 6, 2) = '11' THEN 'NOV'
                        WHEN substr(DATA_BASE, 6, 2) = '12' THEN 'DEZ'
                    END || '-' || substr(DATA_BASE, 3, 2) AS PERIODO_FORMATADO
                    FROM RAZAO_EBS A
                    INNER JOIN DEPARA_CONTA B ON A.CONTA = B.CONTA
                    WHERE A.HISTORICO LIKE ('%TRANSF.P/ELAB. - Editorial Intercompany - CL_%')
                    AND LANCAMENTO = 'PAC - VARIACAO BRL'
                    AND A.EMPRESA = '{empresa}'
                    AND A.CONTA IN (3210180995)
                    AND EMPRESA = '139'
                    AND DATE(DATA_BASE) BETWEEN '{dt_ini_para_extracao}' AND  '{dt_fim_para_extracao}'
                    """
                    print(f"Executando query para empresa {empresa}")
                    # Executar a query e armazenar os resultados da base analítica grades
                    df_analitica_razao_empresa = pd.read_sql(
                        query_base_analitica_razao, conn)
                    print(
                        f"Query executada com sucesso para empresa {empresa}")

                    # Adicionar a coluna de ID sequencial
                    df_analitica_razao_empresa['ID'] = pd.Series(
                        range(1, len(df_analitica_razao_empresa) + 1))

                    # Verificar se a coluna Período existe
                    if 'PERIODO_FORMATADO' in df_analitica_razao_empresa.columns:
                        # Obter os períodos distintos
                        periodos_distintos = df_analitica_razao_empresa['PERIODO_FORMATADO'].unique(
                        )
                        print(
                            f"Períodos distintos encontrados para empresa {empresa}: {periodos_distintos}")

                        for periodo in periodos_distintos:
                            # Filtrar o DataFrame para o período atual
                            df_periodo = df_analitica_razao_empresa[
                                df_analitica_razao_empresa['PERIODO_FORMATADO'] == periodo]

                            # Definir o nome do arquivo
                            file_name = f"BASE_ANALITICA_RAZAO_{empresa}_{periodo}.xlsx"
                            file_path = os.path.join(
                                arquivos_analitico_razao, file_name)

                            # Salvar o DataFrame em um arquivo Excel
                            df_periodo.to_excel(file_path, index=False)
                            print(
                                f"{Fore.GREEN}Arquivo {file_name} salvo com sucesso!")
                    else:
                        print(
                            f"{Fore.RED}A coluna PERIODO não existe na consulta para a empresa {empresa}.")
                except Exception as e:
                    print(
                        f"{Fore.RED}Erro ao exportar a BASE_ANALITICA_RAZAO_{empresa}: {e}")
                # Atualizar a barra de progresso
                bar()
    except Exception as e:
        print(f"{Fore.RED}Erro ao executar a atualização: {e}")


# Fechar a conexão
conn.close()
conn_TP215.dispose()

# Registrar o tempo de término
end_time = time.time()

# Calcular e imprimir o tempo de execução
execution_time = end_time - start_time
hours, rem = divmod(execution_time, 3600)
minutes, seconds = divmod(rem, 60)
print(f"{BLUE}Tempo de execução: {int(hours):02}:{
      int(minutes):02}:{int(seconds):02}")

# Encerrar o script
sys.exit()
