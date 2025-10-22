import pandas as pd
import os

def ler_CSV(arquivo):
    """
    Lê um arquivo CSV e retorna um DataFrame
    """
    # Verifica se o arquivo existe
    if not os.path.isfile(arquivo):
        raise FileNotFoundError(f"O arquivo {arquivo} não foi encontrado.")

    # Lê o arquivo CSV
    try:
        df = pd.read_csv(arquivo, dtype=str)
        print(f"✅ Arquivo CSV carregado com sucesso!")
        print(f"📊 Dimensões: {df.shape}")
        print(f"📋 Colunas: {list(df.columns)}")
        return df
    except Exception as e:
        raise ValueError(f"Erro ao ler o arquivo {arquivo}: {e}")


def tratar_vendas_confeitaria():
    """
    Lê e trata os dados de vendas da confeitaria, agrupando por ano e mês
    """
    print("="*70)
    print("TRATAMENTO DE DADOS - VENDAS CONFEITARIA")
    print("="*70)
    
    # Caminho correto para o arquivo
    arquivo_vendas = r"c:\Users\JuuhF\Downloads\dados-projeto\AB-Analise-Dados\analise-dados\data\raw\vendas_confeitaria.csv"
    
    try:
        # Ler o arquivo CSV
        print("\n1. CARREGANDO DADOS:")
        print("-" * 30)
        df_vendas = ler_CSV(arquivo_vendas)
        
        # Mostrar dados brutos
        print(f"\n📖 Primeiras 10 linhas dos dados brutos:")
        print(df_vendas.head(10))
        
        print(f"\n📊 Informações dos dados brutos:")
        print(df_vendas.info())
        
        # Verificar se as colunas necessárias existem
        print(f"\n2. ANÁLISE DAS COLUNAS:")
        print("-" * 30)
        colunas_necessarias = ['data', 'valor_unitario']  # Ajustar conforme necessário
        
        print(f"Colunas disponíveis: {list(df_vendas.columns)}")
        
        # Identificar colunas de data e valor
        coluna_data = None
        coluna_valor = None
        
        for col in df_vendas.columns:
            col_lower = col.lower()
            if 'data' in col_lower or 'date' in col_lower:
                coluna_data = col
            elif 'valor' in col_lower or 'price' in col_lower or 'preco' in col_lower:
                coluna_valor = col
        
        print(f"🗓️ Coluna de data identificada: {coluna_data}")
        print(f"💰 Coluna de valor identificada: {coluna_valor}")
        
        if not coluna_data or not coluna_valor:
            print("❌ Erro: Não foi possível identificar as colunas de data e valor automaticamente")
            print("Colunas disponíveis:", list(df_vendas.columns))
            return None
        
        # Converter coluna de data
        print(f"\n3. PROCESSAMENTO DOS DADOS:")
        print("-" * 30)
        
        print(f"Convertendo coluna {coluna_data} para datetime...")
        df_vendas[coluna_data] = pd.to_datetime(df_vendas[coluna_data], errors='coerce')
        
        # Verificar conversão de data
        datas_invalidas = df_vendas[coluna_data].isna().sum()
        if datas_invalidas > 0:
            print(f"⚠️ Atenção: {datas_invalidas} datas inválidas encontradas")
            # Remover linhas com datas inválidas
            df_vendas = df_vendas.dropna(subset=[coluna_data])
            print(f"📊 Dados após remoção de datas inválidas: {df_vendas.shape}")
        
        # Converter coluna de valor para numérico
        print(f"Convertendo coluna {coluna_valor} para numérico...")
        df_vendas[coluna_valor] = pd.to_numeric(df_vendas[coluna_valor], errors='coerce')
        
        # Verificar conversão de valor
        valores_invalidos = df_vendas[coluna_valor].isna().sum()
        if valores_invalidos > 0:
            print(f"⚠️ Atenção: {valores_invalidos} valores inválidos encontrados")
            # Remover linhas com valores inválidos
            df_vendas = df_vendas.dropna(subset=[coluna_valor])
            print(f"📊 Dados após remoção de valores inválidos: {df_vendas.shape}")
        
        # Extrair ano e mês
        df_vendas['Ano'] = df_vendas[coluna_data].dt.year
        df_vendas['Mes'] = df_vendas[coluna_data].dt.month
        
        # Criar campo Ano_Mes (formato YYYYMM)
        df_vendas['Ano_Mes'] = df_vendas['Ano'] * 100 + df_vendas['Mes']
        
        # Calcular valor total por venda (valor_unitario * quantidade)
        # Primeiro vamos converter quantidade para numérico
        coluna_quantidade = None
        for col in df_vendas.columns:
            if 'quantidade' in col.lower() or 'qty' in col.lower() or 'quant' in col.lower():
                coluna_quantidade = col
                break
        
        if coluna_quantidade:
            print(f"📦 Coluna de quantidade identificada: {coluna_quantidade}")
            df_vendas[coluna_quantidade] = pd.to_numeric(df_vendas[coluna_quantidade], errors='coerce')
            
            # Calcular valor total da venda (valor_unitario * quantidade)
            df_vendas['Valor_Total_Venda'] = df_vendas[coluna_valor] * df_vendas[coluna_quantidade]
            print(f"✅ Coluna Valor_Total_Venda criada (valor_unitario × quantidade)")
        else:
            # Se não tiver quantidade, usar apenas valor unitário
            df_vendas['Valor_Total_Venda'] = df_vendas[coluna_valor]
            print(f"⚠️ Coluna quantidade não encontrada, usando apenas valor_unitario")
        
        print(f"✅ Colunas Ano, Mes e Ano_Mes criadas com sucesso!")
        
        # Agrupar por Ano_Mes e somar valores
        print(f"\n4. AGRUPAMENTO POR ANO E MÊS:")
        print("-" * 30)
        
        # Agregações mais detalhadas
        agregacoes = {
            'Valor_Total_Venda': ['sum', 'count', 'mean'],
            coluna_valor: ['mean', 'max', 'min']
        }
        
        if coluna_quantidade:
            agregacoes[coluna_quantidade] = ['sum', 'mean']
        
        df_agrupado = df_vendas.groupby(['Ano', 'Mes', 'Ano_Mes']).agg(agregacoes).round(2)
        
        # Flatten das colunas com nomes mais descritivos
        colunas_novas = []
        for col_nivel1, col_nivel2 in df_agrupado.columns:
            if col_nivel1 == 'Valor_Total_Venda':
                if col_nivel2 == 'sum':
                    colunas_novas.append('Valor_Total_Mes')
                elif col_nivel2 == 'count':
                    # número de transações no mês
                    colunas_novas.append('Numero_Transacoes')
                elif col_nivel2 == 'mean':
                    colunas_novas.append('Valor_Medio_Por_Transacao')
            elif col_nivel1 == coluna_valor:
                if col_nivel2 == 'mean':
                    colunas_novas.append('Valor_Unitario_Medio')
                elif col_nivel2 == 'max':
                    colunas_novas.append('Valor_Unitario_Max')
                elif col_nivel2 == 'min':
                    colunas_novas.append('Valor_Unitario_Min')
            elif col_nivel1 == coluna_quantidade:
                if col_nivel2 == 'sum':
                    colunas_novas.append('Total_Itens_Vendidos')
                elif col_nivel2 == 'mean':
                    colunas_novas.append('Itens_Medios_Por_Transacao')
            else:
                colunas_novas.append(f"{col_nivel1}_{col_nivel2}")
        
        df_agrupado.columns = colunas_novas
        df_agrupado = df_agrupado.reset_index()
        
        print(f"✅ Agrupamento concluído!")
        print(f"📊 Dimensões do resultado: {df_agrupado.shape}")
        
        # Mostrar resultado
        print(f"\n5. RESULTADO DO AGRUPAMENTO:")
        print("-" * 30)
        print(f"Primeiras 15 linhas do agrupamento:")
        print(df_agrupado.head(15))
        
        print(f"\nÚltimas 10 linhas do agrupamento:")
        print(df_agrupado.tail(10))
        
        # Estatísticas resumidas
        print(f"\n6. ESTATÍSTICAS RESUMIDAS:")
        print("-" * 30)
        print(f"📅 Período: {df_agrupado['Ano'].min()} a {df_agrupado['Ano'].max()}")
        print(f"📊 Total de meses: {len(df_agrupado)}")
        print(f"💰 Valor total geral: R$ {df_agrupado['Valor_Total_Mes'].sum():,.2f}")
        print(f"📈 Valor médio por mês: R$ {df_agrupado['Valor_Total_Mes'].mean():,.2f}")
        print(f"🔝 Maior valor mensal: R$ {df_agrupado['Valor_Total_Mes'].max():,.2f}")
        print(f"🔻 Menor valor mensal: R$ {df_agrupado['Valor_Total_Mes'].min():,.2f}")
        print(f"🛒 Total de vendas: {df_agrupado['Quantidade_Vendas'].sum():,}")
        
        if 'Quantidade_Total' in df_agrupado.columns:
            print(f"📦 Total de itens vendidos: {df_agrupado['Quantidade_Total'].sum():,}")
        
        # Mostrar top 5 meses com maiores vendas
        print(f"\n📈 TOP 5 MESES COM MAIORES VENDAS:")
        top_vendas = df_agrupado.nlargest(5, 'Valor_Total_Mes')[['Ano_Mes', 'Ano', 'Mes', 'Valor_Total_Mes', 'Quantidade_Vendas']]
        for _, row in top_vendas.iterrows():
            print(f"  {int(row['Mes']):02d}/{int(row['Ano'])}: R$ {row['Valor_Total_Mes']:,.2f} ({int(row['Quantidade_Vendas'])} vendas) (Ano_Mes: {int(row['Ano_Mes'])})")
        
        return df_agrupado
        
    except FileNotFoundError as e:
        print(f"❌ Erro: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        return None


def salvar_vendas_tratadas_csv(df_vendas, nome_arquivo="vendas_confeitaria_tratadas.csv"):
    """
    Salva os dados de vendas tratados em CSV
    """
    if df_vendas is None or df_vendas.empty:
        print("❌ Erro: DataFrame está vazio ou é None")
        return None
    
    # Diretório de saída
    diretorio_saida = r"c:\Users\JuuhF\Downloads\dados-projeto\AB-Analise-Dados\data\processed"
    os.makedirs(diretorio_saida, exist_ok=True)
    
    # Caminho completo
    caminho_arquivo = os.path.join(diretorio_saida, nome_arquivo)
    
    try:
        # Salvar CSV
        df_vendas.to_csv(caminho_arquivo, index=False, encoding='utf-8')
        
        print(f"\n7. SALVAMENTO DOS DADOS:")
        print("-" * 30)
        print(f"✅ Arquivo salvo: {nome_arquivo}")
        print(f"📁 Localização: {caminho_arquivo}")
        print(f"📊 Registros salvos: {len(df_vendas)}")
        print(f"💾 Tamanho: {os.path.getsize(caminho_arquivo):,} bytes")
        
        return caminho_arquivo
        
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")
        return None


if __name__ == "__main__":
    # Executar tratamento
    df_resultado = tratar_vendas_confeitaria()
    
    # Salvar resultado
    if df_resultado is not None:
        arquivo_salvo = salvar_vendas_tratadas_csv(df_resultado)
        
        if arquivo_salvo:
            print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
            print(f"📄 Dados tratados salvos em: {os.path.basename(arquivo_salvo)}")
            print(f"🚀 Pronto para análises e dashboards!")
        else:
            print(f"\n⚠️ Processamento concluído, mas houve erro no salvamento")
    else:
        print(f"\n❌ Erro no processamento dos dados")


    