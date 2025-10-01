import os
import requests
import pandas as pd
from io import StringIO

# URLs dos diretórios raw no GitHub (terminam em /)
base_urls = {
    'nacional': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/nacional/csv/',
    'estadual': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/estadual/csv/',
    'municipal': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/municipal/csv/',
    'facultativo': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/facultativo/csv/'
}
anos = ['2024', '2025']

pasta_destino = os.path.dirname(__file__)
pasta_feriados = os.path.join(pasta_destino, 'feriados')
os.makedirs(pasta_feriados, exist_ok=True)

todos_feriados_sp = []

def encontrar_url_valida(base_url, ano, categoria):
    nome = f'{ano}.csv'
    url = base_url + nome
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200 and resp.text.strip():
            return url, resp.text
    except requests.RequestException:
        pass
    return None, None

for ano in anos:
    for categoria, base_url in base_urls.items():
        print(f'Procurando arquivo para {categoria} {ano} ...')
        url, text = encontrar_url_valida(base_url, ano, categoria)
        if not url:
            print(f'Nenhum arquivo válido encontrado para {categoria} {ano} em {base_url}')
            continue
        print(f'Baixando {url} ...')
        try:
            # Detecta primeira linha não vazia
            first_line = next((ln for ln in text.splitlines() if ln.strip()), '')
            looks_like_header = ',' in first_line and any(ch.isalpha() for ch in first_line.replace(',', ''))
            if looks_like_header:
                df = pd.read_csv(StringIO(text), dtype=str)
            else:
                ncols = first_line.count(',') + 1 if first_line else 0
                default_cols = ['data', 'nome', 'titulo', 'descricao', 'uf', 'municipio']
                names = default_cols[:ncols] if ncols > 0 else default_cols
                df = pd.read_csv(StringIO(text), header=None, names=names, dtype=str)

            # Normaliza nomes de colunas
            df.columns = [str(c).strip().lower() for c in df.columns]

            df_padronizado = pd.DataFrame()

            # Data
            date_candidates = ['data', 'date']
            date_col = next((c for c in date_candidates if c in df.columns), None)
            if date_col:
                df_padronizado['Data'] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
            else:
                df_padronizado['Data'] = [''] * len(df)

            # Nome do feriado
            nome_candidates = ['nome', 'nome_feriado', 'holiday']
            nome_col = next((c for c in nome_candidates if c in df.columns), None)
            if nome_col:
                nome_series = df[nome_col].fillna('').astype(str).str.strip()
            else:
                nome_series = pd.Series([''] * len(df), index=df.index)
            df_padronizado['Nome_Feriado'] = nome_series

            # Título: usa coluna de título se existir; senão usa Nome_Feriado
            titulo_candidates = ['titulo', 'title']
            titulo_col = next((c for c in titulo_candidates if c in df.columns), None)
            if titulo_col:
                titulo_series = df[titulo_col].fillna('').astype(str).str.strip()
            else:
                # fallback para o nome do feriado
                titulo_series = nome_series.copy()
            
            # Aplica Title Case apenas em valores não vazios
            titulo_series = titulo_series.apply(lambda x: x.title() if x.strip() else x)
            df_padronizado['Titulo'] = titulo_series

            # Tipo
            df_padronizado['Tipo_Feriado'] = [categoria] * len(df)

            # Descrição
            descricao_candidates = ['descricao', 'description']
            descricao_col = next((c for c in descricao_candidates if c in df.columns), None)
            df_padronizado['Descrição'] = df[descricao_col].fillna('') if descricao_col else [''] * len(df)

            # Sigla do estado
            if 'uf' in df.columns:
                df_padronizado['Sigla_Estado'] = df['uf'].fillna('')
            elif 'sigla_estado' in df.columns:
                df_padronizado['Sigla_Estado'] = df['sigla_estado'].fillna('')
            else:
                df_padronizado['Sigla_Estado'] = [''] * len(df)

            # Município
            if categoria == 'nacional':
                df_padronizado['Municipio'] = [''] * len(df)
            else:
                df_padronizado['Municipio'] = df.get('municipio', pd.Series([''] * len(df))).fillna('')

            # Limpa espaços em branco em strings
            df_padronizado = df_padronizado.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # Garante ordem e presença das colunas
            cols = ['Data', 'Nome_Feriado', 'Titulo', 'Tipo_Feriado', 'Descrição', 'Sigla_Estado', 'Municipio']
            for c in cols:
                if c not in df_padronizado.columns:
                    df_padronizado[c] = ''
            df_padronizado = df_padronizado[cols]

            # Remove linhas totalmente vazias e exige pelo menos Data ou Nome_Feriado
            df_filled = df_padronizado.fillna('').astype(str)
            mask_any = df_filled.apply(lambda row: any(v.strip() != '' for v in row), axis=1)
            mask_data = df_filled['Data'].str.strip() != ''
            mask_nome = df_filled['Nome_Feriado'].str.strip() != ''
            df_padronizado = df_padronizado[mask_any & (mask_data | mask_nome)].copy()

            if df_padronizado.empty:
                print(f'Nenhum feriado válido para {categoria} {ano}, pulando.')
            else:
                # Remove duplicados básicos antes de gravar
                df_padronizado.drop_duplicates(subset=['Data', 'Nome_Feriado', 'Sigla_Estado', 'Municipio'], inplace=True)

                # Escreve/concatena em um único arquivo CSV
                arquivo_unico = os.path.join(pasta_feriados, 'todos_feriados.csv')
                write_header = not os.path.exists(arquivo_unico)
                df_padronizado.to_csv(arquivo_unico, mode='a', index=False, header=write_header, encoding='utf-8-sig')

                # Mantém também na lista em memória, caso precise manipular depois
                todos_feriados_sp.append(df_padronizado)

                print(f'Dados de {categoria} {ano} carregados ({len(df_padronizado)} linhas)')
            
        except Exception as e:
            print(f'Erro ao processar {url}: {e}')

# Consolidar arquivo final removendo duplicatas
if os.path.exists(os.path.join(pasta_feriados, 'todos_feriados.csv')):
    df_final = pd.read_csv(os.path.join(pasta_feriados, 'todos_feriados.csv'))
    df_final.drop_duplicates(inplace=True)
    df_final.to_csv(os.path.join(pasta_feriados, 'feriados_completo.csv'), index=False)
    print(f'Arquivo final consolidado: {len(df_final)} feriados únicos salvos em feriados_completo.csv')
