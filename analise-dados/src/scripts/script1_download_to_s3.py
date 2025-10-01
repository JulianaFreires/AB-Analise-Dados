import json
import os
import requests
import pandas as pd
from io import StringIO
import boto3
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Script 1: Baixa dados de feriados da internet e salva no S3
    """
    
    # Configuração do bucket de destino
    BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
    if not BUCKET_NAME:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Variável S3_BUCKET_NAME não configurada'})
        }
    
    # URLs dos dados de feriados
    base_urls = {
        'nacional': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/nacional/csv/',
        'estadual': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/estadual/csv/',
        'municipal': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/municipal/csv/',
        'facultativo': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/facultativo/csv/'
    }
    
    anos = ['2024', '2025', '2026']
    colunas_padrao = ['Data', 'Nome_Feriado', 'Tipo_Feriado', 'Descricao', 'Sigla_Estado', 'Municipio']
    
    # Cliente S3
    s3_client = boto3.client('s3')
    
    relatorio = {
        'processados': 0,
        'erros': 0,
        'arquivos_salvos': [],
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info("🌐 Baixando dados da internet para S3")
    
    # Baixar e salvar cada arquivo
    for categoria, base_url in base_urls.items():
        for ano in anos:
            try:
                url = f'{base_url}{ano}.csv'
                logger.info(f"Baixando: {url}")
                
                # Baixar da internet
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Processar dados básicos
                df = pd.read_csv(StringIO(response.text))
                df.columns = colunas_padrao[:len(df.columns)]
                
                # Garantir colunas padrão
                for coluna in colunas_padrao:
                    if coluna not in df.columns:
                        df[coluna] = ''
                
                df = df[colunas_padrao]
                
                # Salvar no S3
                chave_s3 = f"feriados-raw/{categoria}_{ano}.csv"
                csv_content = df.to_csv(index=False)
                
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=chave_s3,
                    Body=csv_content,
                    ContentType='text/csv',
                    Metadata={
                        'fonte': 'github-feriados-brasil',
                        'categoria': categoria,
                        'ano': ano,
                        'processado_em': datetime.now().isoformat()
                    }
                )
                
                relatorio['arquivos_salvos'].append(chave_s3)
                relatorio['processados'] += 1
                
                logger.info(f"✅ Salvo no S3: {chave_s3} ({len(df)} registros)")
                
            except Exception as e:
                logger.error(f"❌ Erro baixando {categoria} {ano}: {str(e)}")
                relatorio['erros'] += 1
    
    # Resposta
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Download concluído',
            'bucket_destino': BUCKET_NAME,
            'relatorio': relatorio
        }, indent=2)
    }