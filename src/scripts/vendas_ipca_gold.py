"""
Gera a base gold unindo IPCA e Vendas (inner join por Ano_Mes)
Mantém apenas as colunas solicitadas:
 - Ano_Mes
 - variacao_mensal
 - variacao_anual
 - Numero_Transacoes (ou Quantidade_Vendas)
 - Valor_Medio_Por_Venda (ou Valor_Medio_Por_Transacao)
 - Valor_Total_Mes
 - Total_Itens_Vendidos (ou Quantidade_Total)

Saída: data/processed/base_gold_ipca_vendas.csv
"""

import os
import pandas as pd


def localizar_arquivo_processed(nome):
    base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    caminho = os.path.join(base, 'data', 'processed', nome)
    return caminho


def carregar_csv(caminho):
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    return pd.read_csv(caminho)


def padronizar_colunas_vendas(df):
    # normaliza nomes: remove espaços e deixa lower para busca
    cols_map = {c: c for c in df.columns}
    lc = {c.lower(): c for c in df.columns}

    # map possíveis nomes
    def encontrar(*candidatos):
        for cand in candidatos:
            if cand.lower() in lc:
                return lc[cand.lower()]
        return None

    # candidatos para cada campo
    num_trans = encontrar('Numero_Transacoes', 'Quantidade_Vendas', 'quantidade_vendas')
    total_itens = encontrar('Total_Itens_Vendidos', 'Quantidade_Total', 'quantidade_total')
    valor_medio = encontrar('Valor_Medio_Por_Venda', 'Valor_Medio_Por_Transacao', 'valor_medio_por_venda')
    valor_total = encontrar('Valor_Total_Mes', 'valor_total_mes')

    # also ensure Ano_Mes present
    ano_mes_col = encontrar('Ano_Mes', 'ano_mes')

    # build standardized df
    std = pd.DataFrame()
    if ano_mes_col is None:
        # try to build from Ano/Mes
        ano_col = encontrar('Ano', 'ano', 'year')
        mes_col = encontrar('Mes', 'mes', 'month')
        if ano_col and mes_col:
            std['Ano_Mes'] = df[ano_col].astype(int) * 100 + df[mes_col].astype(int)
        else:
            raise KeyError('Não foi possível localizar coluna Ano_Mes nem Ano/Mes nas vendas')
    else:
        std['Ano_Mes'] = df[ano_mes_col].astype(int)

    # pull in requested columns if exist, else fill NA
    std['Numero_Transacoes'] = df[num_trans] if num_trans in df.columns else pd.NA
    std['Total_Itens_Vendidos'] = df[total_itens] if total_itens in df.columns else pd.NA
    std['Valor_Medio_Por_Venda'] = df[valor_medio] if valor_medio in df.columns else pd.NA
    std['Valor_Total_Mes'] = df[valor_total] if valor_total in df.columns else pd.NA

    # ensure numeric types where possible
    for col in ['Numero_Transacoes', 'Total_Itens_Vendidos', 'Valor_Medio_Por_Venda', 'Valor_Total_Mes']:
        if col in std.columns:
            std[col] = pd.to_numeric(std[col], errors='coerce')

    return std


def padronizar_colunas_ipca(df):
    # ensure Ano_Mes exists
    if 'Ano_Mes' not in df.columns and 'ano' in df.columns and 'mes' in df.columns:
        df['Ano_Mes'] = df['ano'].astype(int) * 100 + df['mes'].astype(int)

    # select columns
    out = pd.DataFrame()
    if 'Ano_Mes' not in df.columns:
        raise KeyError('IPCA sem coluna Ano_Mes')
    out['Ano_Mes'] = df['Ano_Mes'].astype(int)

    # map variacao_mensal/variacao_anual
    if 'variacao_mensal' in df.columns:
        out['variacao_mensal'] = pd.to_numeric(df['variacao_mensal'], errors='coerce')
    else:
        out['variacao_mensal'] = pd.NA

    if 'variacao_anual' in df.columns:
        out['variacao_anual'] = pd.to_numeric(df['variacao_anual'], errors='coerce')
    else:
        out['variacao_anual'] = pd.NA

    return out


def criar_base_gold():
    processed_dir = os.path.dirname(localizar_arquivo_processed(''))
    ipca_path = localizar_arquivo_processed('ipca_processado.csv')
    vendas_path = localizar_arquivo_processed('vendas_confeitaria_tratadas.csv')

    print('🔍 Carregando IPCA de:', ipca_path)
    ipca = carregar_csv(ipca_path)
    print('🔍 Carregando Vendas de:', vendas_path)
    vendas = carregar_csv(vendas_path)

    # padroniza
    ipca_std = padronizar_colunas_ipca(ipca)
    vendas_std = padronizar_colunas_vendas(vendas)

    # remover duplicações por Ano_Mes
    ipca_std = ipca_std.drop_duplicates(subset=['Ano_Mes'])
    vendas_std = vendas_std.drop_duplicates(subset=['Ano_Mes'])

    # join inner
    print('🔗 Fazendo inner join por Ano_Mes...')
    df_gold = pd.merge(ipca_std, vendas_std, on='Ano_Mes', how='inner')

    # selecionar colunas finais na ordem pedida
    cols_finais = ['Ano_Mes', 'variacao_mensal', 'variacao_anual', 'Numero_Transacoes', 'Valor_Medio_Por_Venda', 'Valor_Total_Mes', 'Total_Itens_Vendidos']
    # Alguns podem não existir; filtrar existentes
    cols_existentes = [c for c in cols_finais if c in df_gold.columns]
    df_gold = df_gold[cols_existentes]

    # salvar
    out_path = localizar_arquivo_processed('tabela_gold_ipca_vendas.csv')
    df_gold.to_csv(out_path, index=False, encoding='utf-8')
    print(f'💾 Base gold salva em: {out_path} (linhas: {len(df_gold)})')

    return df_gold


if __name__ == '__main__':
    try:
        df = criar_base_gold()
    except Exception as e:
        print('Erro ao criar base gold:', e)