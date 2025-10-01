"""
Utilitários para processamento de dados
"""

import pandas as pd
import os
import gzip
from io import StringIO
from typing import Optional, Dict, Any

def verificar_estrutura_diretorios() -> None:
    """
    Verifica e cria a estrutura de diretórios necessária
    """
    diretorios = [
        'data/raw',
        'data/processed', 
        'data/external',
        'reports',
        'docs'
    ]
    
    for diretorio in diretorios:
        os.makedirs(diretorio, exist_ok=True)
        print(f"✅ Diretório verificado: {diretorio}")

def carregar_arquivo_comprimido(caminho_arquivo: str, 
                               encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Carrega arquivo CSV comprimido (.gz)
    
    Args:
        caminho_arquivo: Caminho para o arquivo .gz
        encoding: Codificação do arquivo
        
    Returns:
        DataFrame com os dados carregados
    """
    try:
        with gzip.open(caminho_arquivo, 'rt', encoding=encoding) as arquivo:
            conteudo = arquivo.read()
        
        # Trata valores ausentes do IBGE
        conteudo = conteudo.replace('..', '')
        
        return pd.read_csv(StringIO(conteudo))
    
    except Exception as e:
        print(f"❌ Erro ao carregar {caminho_arquivo}: {e}")
        return pd.DataFrame()

def gerar_relatorio_dados(df: pd.DataFrame, 
                         nome_dataset: str) -> Dict[str, Any]:
    """
    Gera relatório de qualidade dos dados
    
    Args:
        df: DataFrame para análise
        nome_dataset: Nome do conjunto de dados
        
    Returns:
        Dicionário com estatísticas do dataset
    """
    if df.empty:
        return {"erro": "DataFrame vazio"}
    
    relatorio = {
        "dataset": nome_dataset,
        "registros": len(df),
        "colunas": len(df.columns),
        "memoria_mb": df.memory_usage(deep=True).sum() / 1024**2,
        "completude": ((df.count().sum() / (len(df) * len(df.columns))) * 100),
        "valores_ausentes": df.isnull().sum().sum(),
        "tipos_dados": df.dtypes.to_dict()
    }
    
    return relatorio

def salvar_com_backup(df: pd.DataFrame, 
                     caminho: str, 
                     criar_backup: bool = True) -> bool:
    """
    Salva DataFrame com opção de backup
    
    Args:
        df: DataFrame para salvar
        caminho: Caminho de destino
        criar_backup: Se deve criar backup do arquivo existente
        
    Returns:
        True se salvou com sucesso
    """
    try:
        # Criar backup se arquivo existir
        if criar_backup and os.path.exists(caminho):
            backup_path = f"{caminho}.backup"
            os.rename(caminho, backup_path)
            print(f"📦 Backup criado: {backup_path}")
        
        # Salvar novo arquivo
        df.to_csv(caminho, index=False, encoding='utf-8')
        print(f"✅ Arquivo salvo: {caminho}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao salvar {caminho}: {e}")
        return False