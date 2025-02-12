import os
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from alive_progress import alive_bar
import time
import re
import sys  # Importa o módulo sys para encerrar o script
from openpyxl import load_workbook
import cx_Oracle as oracle
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import numpy as np
import glob
import warnings
from colorama import Fore, Style, init
from sas7bdat import SAS7BDAT

print("""
-------------------->> CONTROLTECH <<--------------------
AUTOR: --
DATA DE CRIAÇÃO: 2025.05
""")

# Cores para o print
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"

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
    # Apontamentos na minha máquina, para quando não tiver acesso na rede, ajuste necessário também na variaeceis do ambiente
    # os.environ['OCI_LIB64'] = r'C:\oracle\instantclient_23_6'p
    # os.environ['PATH'] = r'C:\oracle\instantclient_23_6;' + os.environ['PATH']
    print(f"{GREEN}Variáveis de ambiente configuradas com sucesso!")
except Exception as e:
    print(f"{RED}Falha ao configurar as variáveis de ambiente: %s" % e)
    sys.exit(1)  # Encerra o script em caso de falha

# Variáveis de configuração
variaveis_de_configuracao = {
    'DT_INI': '01/08/2024',  # DATA INICIAL DO PERÍODO DE ATUALIZAÇÃO
    'DT_FIM': '31/08/2024'  # DATA FINAL DO PERÍODO DE ATUALIZAÇÃO
}


# Registrar o tempo de término
end_time = time.time()
elapsed_time = end_time - start_time

# Exibir o tempo total de execução
print(f"{BLUE}Tempo total de execução: {elapsed_time:.2f} segundos")

# Variáveis de controle
variaveis_de_controle = {
    # DEPARA e BASES COMPLEMENTARES
    'importa_deparas': True,
    'importa_base_centro_ebs': True,
    # BASE BALANCETE EBS
    'lista_balancete_ebs': True,
    'importa_balancete_ebs': True,
    # BASE RAZÃO EBS
    'importa_razao_ebs': True,
    'update_razao_ebs': True,
    # BASE PAC
    'importa_base_pac': False,
    'ajuste_manual_pac': False,
    # CALCULOS CPV
    'calcula_cpv': True,
    # CALCULOS COMPRAS CPV
    'calcula_compras_cpv': True,
    # DEMONSTRATIVO CPV
    'gera_demonstrativo_cpv': True,
    # BASES ANALITCAS
    'exporta_base_analitica': True,
    # VALIDAÇÃO CMV
    'validacao_cmv': False,
    # SAS
    'importa_sas': False
}

# Caminho do banco de dados
pasta_banco_dados = os.path.join(
    os.path.expanduser('~'),
    'OneDrive - EDITORA E DISTRIBUIDORA EDUCACIONAL S A',
    '02- CONTROLADORIA',
    '01- BANCO_CPV_SQLITE'
)

# Conecte-se ao banco de dados SQLite se não exister crie um novo
conn = sqlite3.connect(os.path.join(pasta_banco_dados, 'CPV.sqlite'))

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
                              index=False, dtype={'CENTRO': 'INTEGER'})

        print(f"{GREEN}Base de Centros EBS importada com sucesso!")

    except Exception as e:
        print(f"{RED}Erro ao importar a base de centros EBS: {e}")

    # Cria índice para a coluna CENTRO na tabela BASE_CENTROS_SAP
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_centro_ebs ON BASE_CENTROS_EBS(CENTRO_EBS);")
    conn.commit()

# Lista os arquivos do Balancete EBS -- ARQUIVOS COM TODOS ANALITICOS POR MES - PREPARA OS ARQUIVOS PARA IMPORTAÇÃO
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

    # Função para converter o período de mmm/yy para mm/yyyy
    def converter_periodo(periodo):
        return datetime.strptime(periodo, '%b/%y').strftime('%m/%Y')
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
                                      'data_arquivo': [data_arquivo]})], ignore_index=True)
                    bar()  # Atualiza a barra de progresso

    # Salva o dataframe no sqlite
    lista_balancete_ebs.to_sql(
        'LISTA_BALANCETE_EBS', conn, if_exists='replace', index=False)

else:
    lista_balancete_ebs = pd.read_sql(
        "select * from LISTA_BALANCETE_EBS", conn)


# Importa o Balancete EBS -- IMPORTA OS ARQUIVOS PARA O BANCO DE DADOS
if variaveis_de_controle['importa_balancete_ebs']:

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
                   SELECT o105514.EMPRESA
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

# Importa a base de  --   Atualizando base PAC -- PELO ORACLE Insereindo dados por conta e empresa equivale as bases sas CPV_BI.PAC_ORIG - flow macro do sas
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
                    CENTRO
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

# if variaveis_de_controle['importa_sas']:

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


# Registrar o tempo de término
end_time = time.time()

# Calcular e imprimir o tempo de execução
execution_time = end_time - start_time
hours, rem = divmod(execution_time, 3600)
minutes, seconds = divmod(rem, 60)
print(f"{BLUE}Tempo de execução: {int(hours):02}:{
      int(minutes):02}:{int(seconds):02}")


# Realizando os cálculos ~~ Novo Processo:
if variaveis_de_controle['calcula_cpv']:
    # Criação da tabela BASE_CALCULO_CPV
    print(f"{BLUE}Criando a tabela BASE_CALCULO_CPV")

    try:
        # Query para criar a tabela BASE_CALCULO_CPV
        query_base_calculo_cpv = """
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
        # Executar a query e armazenar os resultados em um DataFrame
        df = pd.read_sql(query_base_calculo_cpv, conn)

        # Criar ou atualizar a tabela
        df.to_sql("BASE_CALCULO_CPV", conn, if_exists="replace", index=False)

        print(f"{GREEN}Tabela BASE_CALCULO_CPV atualizada com sucesso!")

    except Exception as e:
        print(f"{RED}Erro ao criar a tabela BASE_CALCULO_CPV: {e}")

    print(f"{BLUE}Atualizando base de CPV com dados da base RAZAO")

    try:
        # Atualizando a tabela BASE_CALCULO_CPV com dados da base RAZAO
        query_insert_base_calculo_cpv = """
        INSERT INTO BASE_CALCULO_CPV (DATA_BASE, EMPRESA, CONTA, FONTE, CALCULO, VALOR)
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
        # Executar a query de inserção
        conn.execute(query_insert_base_calculo_cpv)
        conn.commit()

        print(
            f"{GREEN}Dados inseridos na tabela BASE_CALCULO_CPV com dados da RAZAO EBS com sucesso!")
    except Exception as e:
        print(f"{RED}Erro ao inserir dados na tabela BASE_CALCULO_CPV: {e}")

    # Exportar a tabela BASE_CALCULO_CPV para um arquivo CSV
    try:
        df_base_calculo_cpv = pd.read_sql_query(
            "SELECT * FROM BASE_CALCULO_CPV", conn)
        caminho_arquivo_xlsx = os.path.join(
            pasta_banco_dados, 'BASE_CALCULO_CPV.xlsx')
        df_base_calculo_cpv.to_csv(caminho_arquivo_xlsx, index=False)
        print(
            f"{GREEN}Tabela BASE_CALCULO_CPV exportada com sucesso para {caminho_arquivo_xlsx}")
    except Exception as e:
        print(f"{RED}Erro ao exportar a tabela BASE_CALCULO_CPV: {e}")

# Fechar a conexão
conn.close()
conn_TP215.dispose()
