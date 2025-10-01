import os
import requests
import pandas as pd
from io import StringIO

# URLs dos arquivos CSV de feriados para 2024 e 2025
base_urls = {
    'nacional': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/nacional/csv/',
    'estadual': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/estadual/csv/',
    'municipal': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/municipal/csv/',
    'facultativo': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/facultativo/csv/'
}
anos = ['2024', '2025']

# Pasta de destino
pasta_destino = os.path.dirname(__file__)
pasta_feriados = os.path.join(pasta_destino, 'feriados')
os.makedirs(pasta_feriados, exist_ok=True)

# Lista para armazenar todos os DataFrames
todos_dataframes = []

for categoria, base_url in base_urls.items():
    for ano in anos:
        url = f'{base_url}{ano}.csv'
        print(f'Baixando {url} ...')
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            
            # Ler o CSV diretamente do conteúdo
            df = pd.read_csv(StringIO(resp.text))
            
            # Definir nomes das colunas padronizados
            colunas_padrao = ['Data', 'Nome_Feriado', 'Tipo_Feriado', 'Descricao', 'Sigla_Estado', 'Municipio']
            
            # Renomear colunas para os nomes padronizados
            num_colunas = len(df.columns)
            if num_colunas >= len(colunas_padrao):
                df.columns = colunas_padrao + list(df.columns[len(colunas_padrao):])
            else:
                df.columns = colunas_padrao[:num_colunas]
                # Adicionar colunas faltantes
                for i in range(num_colunas, len(colunas_padrao)):
                    df[colunas_padrao[i]] = ''
            
            # Manter apenas as colunas padronizadas
            df = df[colunas_padrao]
            
            # Garantir que todas as colunas tenham valores (mesmo que vazios)
            for col in colunas_padrao:
                df[col] = df[col].fillna('')
            
            # Apenas adicionar à lista
            todos_dataframes.append(df)
            print(f'Dados de {categoria} {ano} carregados ({len(df)} registros)')
            
        except Exception as e:
            print(f'Erro ao baixar {url}: {e}')

# Juntar todos os DataFrames em um único arquivo
if todos_dataframes:
    print(f'Combinando {len(todos_dataframes)} DataFrames...')
    df_combinado = pd.concat(todos_dataframes, ignore_index=True)
    print(f'Total de registros: {len(df_combinado)}')
    
    # Salvar arquivo consolidado
    arquivo_final = os.path.join(pasta_feriados, 'feriados_completo.csv')
    df_combinado.to_csv(arquivo_final, index=False)
    print(f'Arquivo consolidado salvo em: {arquivo_final}')
else:
    print('Nenhum dado foi carregado')