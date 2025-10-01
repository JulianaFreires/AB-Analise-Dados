import pandas as pd
import gzip

# Caminho do arquivo
arquivo = 'br_ibge_ipca_mes_brasil.csv.gz'

# Ler o arquivo CSV compactado
def carregar_e_tratar(arquivo):
    with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
        df = pd.read_csv(f)
    # Remove linhas com qualquer valor nulo ou vazio
    # Substitui valores nulos por média para colunas numéricas e mediana para outras
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
            df[col] = df[col].fillna(df[col].mean())
        else:
            df[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown')
    df = df[~df.apply(lambda x: x.astype(str).str.strip().eq('').any(), axis=1)]
    # Ordena por ano e mês, se existirem
    ano_col = next((col for col in df.columns if col.lower() == 'ano'), None)
    mes_col = next((col for col in df.columns if col.lower() == 'mes'), None)
    if ano_col and mes_col:
        df = df.sort_values(by=[ano_col, mes_col], ascending=[True, True])
    # Padroniza formato dos números para evitar notação científica
    for col in df.select_dtypes(include=['float', 'float64', 'float32']):
        df[col] = df[col].apply(lambda x: f'{x:.8f}')
    return df

df_tratado = carregar_e_tratar(arquivo)
df_tratado.to_csv('ipca_tratado.csv', index=False)
print('Base tratada salva em ipca_tratado.csv (sem valores nulos ou vazios)')