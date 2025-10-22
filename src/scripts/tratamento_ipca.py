"""
Tratamento de Dados IPCA - IBGE

Este script realiza o tratamento dos dados do IPCA (Índice de Preços ao Consumidor Amplo) 
do IBGE, incluindo limpeza, ordenação e padronização dos dados.

Autor: Sistema de Análise de Dados
Data: 2025-10-09
"""

import pandas as pd
import gzip
from io import StringIO
import os
from datetime import datetime


def setup_directories():
    """
    Configura e cria os diretórios necessários para o processamento
    """
    # Estrutura de diretórios (Clean Architecture)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    directories = {
        'raw': os.path.join(base_dir, 'data', 'raw'),
        'processed': os.path.join(base_dir, 'data', 'processed'),
        'reports': os.path.join(base_dir, 'reports')
    }
    
    # Criando os diretórios se não existirem
    for name, path in directories.items():
        os.makedirs(path, exist_ok=True)
        print(f"📁 Diretório verificado: {path}")
    
    return directories


def verificar_arquivo_origem(directories):
    """
    Verifica a existência do arquivo de dados IPCA
    """
    arquivo_original = os.path.join(directories['raw'], 'br_ibge_ipca_mes_brasil.csv.gz')
    
    if os.path.exists(arquivo_original):
        tamanho_mb = os.path.getsize(arquivo_original) / (1024 * 1024)
        print(f"✅ Arquivo encontrado: {arquivo_original}")
        print(f"📊 Tamanho: {tamanho_mb:.2f} MB")
        return arquivo_original
    else:
        print(f"❌ Arquivo não encontrado: {arquivo_original}")
        print("💡 Por favor, coloque o arquivo 'br_ibge_ipca_mes_brasil.csv.gz' na pasta 'data/raw/'")
        
        # Verificando se existe na raiz (para compatibilidade)
        arquivo_raiz = os.path.join(directories['raw'], '..', '..', 'br_ibge_ipca_mes_brasil.csv.gz')
        if os.path.exists(arquivo_raiz):
            print("🔄 Encontrado arquivo na raiz do projeto - será usado temporariamente")
            return arquivo_raiz
        
        return None


def carregar_e_tratar(nome_arquivo_comprimido):
    """
    Carrega e trata os dados do IPCA a partir de um arquivo .csv.gz
    
    Parâmetros:
    nome_arquivo_comprimido (str): caminho do arquivo comprimido
    
    Retorna:
    pandas.DataFrame: dados tratados do IPCA
    """
    print(f"🔄 Carregando dados de: {nome_arquivo_comprimido}")
    
    try:
        # Lendo o arquivo comprimido
        with gzip.open(nome_arquivo_comprimido, 'rt', encoding='utf-8') as arquivo:
            conteudo = arquivo.read()
        
        # Tratando valores nulos
        conteudo = conteudo.replace('..', '0')
        
        # Convertendo para DataFrame
        df = pd.read_csv(StringIO(conteudo))
        
        # Limpeza e formatação dos dados
        df = df.dropna(how='all')  # Remove linhas completamente vazias
        
        # Converte colunas numéricas
        colunas_numericas = df.select_dtypes(include=['object']).columns
        for col in colunas_numericas:
            if col not in ['Código', 'Mês', 'Local']:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    pass
        
        # Ordena por ano e mês se as colunas existirem
        if 'Ano' in df.columns and 'Mês' in df.columns:
            df = df.sort_values(['Ano', 'Mês'])
        elif 'ano' in df.columns and 'mes' in df.columns:
            # ordenar por colunas minúsculas caso existam
            df = df.sort_values(['ano', 'mes'])

        # Garantir colunas Ano e Mes no formato esperado pelo pipeline de vendas
        # Aceitamos tanto 'Ano'/'Mês' quanto 'ano'/'mes' e normalizamos para 'Ano' e 'Mes'
        if 'Ano' in df.columns and 'Mês' in df.columns:
            # converter nomes com acento e tipos
            try:
                df['Ano'] = pd.to_numeric(df['Ano'], errors='coerce').astype('Int64')
            except Exception:
                pass
            try:
                df['Mes'] = pd.to_numeric(df['Mês'], errors='coerce').astype('Int64')
            except Exception:
                pass
        elif 'ano' in df.columns and 'mes' in df.columns:
            df['Ano'] = pd.to_numeric(df['ano'], errors='coerce').astype('Int64')
            df['Mes'] = pd.to_numeric(df['mes'], errors='coerce').astype('Int64')
        else:
            # tenta detectar colunas possíveis alternativas (ex: 'year','month')
            ano_col = next((c for c in df.columns if c.lower() in ['ano', 'year']), None)
            mes_col = next((c for c in df.columns if c.lower() in ['mes', 'mês', 'month']), None)
            if ano_col:
                df['Ano'] = pd.to_numeric(df[ano_col], errors='coerce').astype('Int64')
            if mes_col:
                df['Mes'] = pd.to_numeric(df[mes_col], errors='coerce').astype('Int64')

        # Criar campo Ano_Mes no formato YYYYMM (inteiro) se possível
        if 'Ano' in df.columns and 'Mes' in df.columns:
            # Preencher valores faltantes com 0 antes do cálculo é indesejável; remover linhas sem Ano/Mes
            df = df.dropna(subset=['Ano', 'Mes']).copy()
            # garantir inteiros
            df['Ano'] = df['Ano'].astype(int)
            df['Mes'] = df['Mes'].astype(int)
            df['Ano_Mes'] = df['Ano'] * 100 + df['Mes']
            # garantir tipo inteiro
            df['Ano_Mes'] = df['Ano_Mes'].astype(int)
            print("   ✅ Campo Ano_Mes criado no formato YYYYMM")
        else:
            print("   ⚠️ Não foi possível criar Ano_Mes: colunas Ano e/ou Mes não encontradas")
        
        print(f"✅ Dados carregados: {len(df)} registros")
        print(f"📋 Colunas: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        return None


def analise_exploratoria(df):
    """
    Realiza análise exploratória dos dados IPCA
    
    Parâmetros:
    df (pandas.DataFrame): DataFrame com dados do IPCA
    """
    print("\n📊 ANÁLISE EXPLORATÓRIA DOS DADOS IPCA")
    print("=" * 60)
    
    # Informações gerais
    print(f"📈 Número de registros: {len(df):,}")
    print(f"📋 Número de colunas: {len(df.columns)}")
    print(f"🏷️  Colunas: {list(df.columns)}")
    
    # Verificando valores nulos
    print(f"\n🔍 Valores nulos por coluna:")
    nulos = df.isnull().sum()
    for col, count in nulos.items():
        if count > 0:
            print(f"   {col}: {count:,} ({count/len(df)*100:.1f}%)")
        else:
            print(f"   {col}: ✅ Sem valores nulos")
    
    # Informações sobre tipos de dados
    print(f"\n🔢 Tipos de dados:")
    for col, dtype in df.dtypes.items():
        print(f"   {col}: {dtype}")
    
    # Estatísticas descritivas para colunas numéricas
    colunas_numericas = df.select_dtypes(include=['int64', 'float64']).columns
    if len(colunas_numericas) > 0:
        print(f"\n📊 Estatísticas descritivas:")
        stats = df[colunas_numericas].describe()
        print(stats)
        
        # Informações sobre o período dos dados
        if 'Ano' in df.columns:
            ano_min = df['Ano'].min()
            ano_max = df['Ano'].max()
            print(f"\n📅 Período dos dados: {ano_min} a {ano_max}")
    
    return nulos, stats if len(colunas_numericas) > 0 else None


def salvar_dados_tratados(df, directories):
    """
    Salva os dados tratados em arquivo CSV
    
    Parâmetros:
    df (pandas.DataFrame): DataFrame com dados tratados
    directories (dict): Dicionário com caminhos dos diretórios
    """
    arquivo_tratado = os.path.join(directories['processed'], 'ipca_processado.csv')
    try:
        # Salvar apenas o arquivo principal (sem criar backups automáticos)
        df.to_csv(arquivo_tratado, index=False, encoding='utf-8')
        print(f"✅ Dados salvos em: {arquivo_tratado}")

        # Verificar tamanho do arquivo salvo
        tamanho_principal = os.path.getsize(arquivo_tratado) / (1024 * 1024)
        print(f"📊 Tamanho do arquivo: {tamanho_principal:.2f} MB")

        # Verificação da integridade dos dados salvos
        df_verificacao = pd.read_csv(arquivo_tratado, nrows=3)
        print(f"\n🔍 Verificação - Primeiras 3 linhas do arquivo salvo:")
        print(df_verificacao)

        # Retornamos None no lugar do backup para compatibilidade
        return arquivo_tratado, None

    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")
        return None, None


def main():
    """
    Função principal que executa todo o pipeline de tratamento dos dados IPCA
    """
    print("🚀 INICIANDO TRATAMENTO DE DADOS IPCA - IBGE")
    print("=" * 60)
    
    # 1. Configurar diretórios
    directories = setup_directories()
    
    # 2. Verificar arquivo de origem
    arquivo_original = verificar_arquivo_origem(directories)
    
    if not arquivo_original:
        print("❌ Não foi possível localizar o arquivo de dados. Processo interrompido.")
        return None
    
    # 3. Carregar e tratar dados
    df_ipca = carregar_e_tratar(arquivo_original)
    
    if df_ipca is None:
        print("❌ Falha no carregamento dos dados. Processo interrompido.")
        return None
    
    # 4. Análise exploratória
    nulos, stats = analise_exploratoria(df_ipca)
    
    # 5. Salvar dados tratados
    arquivo_principal, arquivo_backup = salvar_dados_tratados(df_ipca, directories)
    
    if arquivo_principal:
        print("\n" + "=" * 60)
        print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        print(f"📁 Arquivo principal: {arquivo_principal}")
        print(f"💾 Arquivo backup: {arquivo_backup}")
        print(f"📊 Total de registros processados: {len(df_ipca):,}")
        print("🎯 Dados prontos para análise!")
    else:
        print("❌ Falha na finalização do processamento.")
    
    return df_ipca


if __name__ == "__main__":
    # Executar o processamento principal
    dados_ipca = main()