import json
import os
import requests
import csv
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
                
                # Processar dados básicos com CSV nativo
                csv_content = response.text
                lines = csv_content.strip().split('\n')
                
                if not lines:
                    logger.warning(f"Arquivo vazio: {url}")
                    continue
                
                # Ler CSV usando módulo csv nativo
                reader = csv.reader(StringIO(csv_content))
                rows = list(reader)
                
                if not rows:
                    logger.warning(f"Nenhuma linha encontrada: {url}")
                    continue
                
                # Primeira linha são os headers originais
                headers_originais = rows[0] if rows else []
                data_rows = rows[1:] if len(rows) > 1 else []
                
                # Mapear para colunas padrão
                processed_rows = []
                for row in data_rows:
                    # Garantir que a linha tenha o número correto de colunas
                    while len(row) < len(colunas_padrao):
                        row.append('')
                    
                    # Pegar apenas as colunas necessárias
                    processed_row = row[:len(colunas_padrao)]
                    processed_rows.append(processed_row)
                
                # Criar CSV final
                output = StringIO()
                writer = csv.writer(output)
                
                # Escrever header padrão
                writer.writerow(colunas_padrao)
                
                # Escrever dados
                writer.writerows(processed_rows)
                
                final_csv_content = output.getvalue()
                output.close()
                
                # Salvar no S3
                chave_s3 = f"feriados-raw/{categoria}_{ano}.csv"
                
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=chave_s3,
                    Body=final_csv_content,
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
                
                logger.info(f"✅ Salvo no S3: {chave_s3} ({len(processed_rows)} registros)")
                
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