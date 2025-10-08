import json
import pandas as pd
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Função handler principal para AWS Lambda
    """
    try:
        resultado = baixar_feriados_brasileiros()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processamento concluído com sucesso',
                'resultado': resultado
            })
        }
    except Exception as e:
        logger.error(f"Erro na execução da Lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

def baixar_feriados_brasileiros():
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    
    if not bucket_name:
        raise ValueError("Variável de ambiente S3_BUCKET_NAME não encontrada")
    
    categorias = {
        'nacional': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/nacional/csv/',
        'estadual': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/estadual/csv/',
        'municipal': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/municipal/csv/',
        'facultativo': 'https://github.com/joaopbini/feriados-brasil/raw/master/dados/feriados/facultativo/csv/'
    }
    
    anos = ['2024', '2025']
    colunas_padrao = ['Data', 'Nome_Feriado', 'Tipo_Feriado', 'Descricao', 'Sigla_Estado', 'Municipio']
    
    s3 = boto3.client('s3')
    sucessos = 0
    erros = 0
    
    logger.info("Iniciando download dos feriados brasileiros...")
    
    for categoria, url_base in categorias.items():
        for ano in anos:
            try:
                url = f'{url_base}{ano}.csv'
                logger.info(f"Baixando feriados {categoria} de {ano}...")
                
                df = pd.read_csv(url)
                
                for coluna in colunas_padrao:
                    if coluna not in df.columns:
                        df[coluna] = ''
                
                df_limpo = df[colunas_padrao].fillna('')
                csv_content = df_limpo.to_csv(index=False)
                nome_arquivo = f"feriados-raw/{categoria}_{ano}.csv"
                
                s3.put_object(
                    Bucket=bucket_name,
                    Key=nome_arquivo,
                    Body=csv_content,
                    ContentType='text/csv'
                )
                
                sucessos += 1
                logger.info(f"{categoria} {ano}: {len(df_limpo)} feriados salvos com sucesso!")
                
            except Exception as e:
                erros += 1
                logger.error(f"Erro ao processar {categoria} {ano}: {e}")
    
    resultado = {
        'sucessos': sucessos,
        'erros': erros,
        'bucket': bucket_name
    }
    
    logger.info(f"Processamento finalizado! Sucessos: {sucessos}, Erros: {erros}")
    return resultado
