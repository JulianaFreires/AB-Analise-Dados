import json
import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Script 2: Pega arquivos de um bucket S3 e transfere para outro bucket
    """
    
    # Configuração dos buckets
    BUCKET_ORIGEM = os.environ.get('S3_BUCKET_ORIGEM')
    BUCKET_DESTINO = os.environ.get('S3_BUCKET_DESTINO')
    
    if not BUCKET_ORIGEM or not BUCKET_DESTINO:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Variáveis S3_BUCKET_ORIGEM e S3_BUCKET_DESTINO devem ser configuradas'
            })
        }
    
    # Cliente S3
    s3_client = boto3.client('s3')
    
    relatorio = {
        'transferidos': 0,
        'erros': 0,
        'arquivos_processados': [],
        'bucket_origem': BUCKET_ORIGEM,
        'bucket_destino': BUCKET_DESTINO,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"🔄 Transferindo de {BUCKET_ORIGEM} para {BUCKET_DESTINO}")
    
    try:
        # Listar arquivos no bucket origem
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_ORIGEM,
            Prefix='feriados-raw/'
        )
        
        if 'Contents' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Nenhum arquivo encontrado no bucket origem'})
            }
        
        # Lista para consolidação
        todos_dataframes = []
        
        # Processar cada arquivo
        for obj in response['Contents']:
            chave_origem = obj['Key']
            
            try:
                logger.info(f"Processando: {chave_origem}")
                
                # Baixar arquivo do bucket origem
                response_obj = s3_client.get_object(
                    Bucket=BUCKET_ORIGEM,
                    Key=chave_origem
                )
                
                # Ler dados
                df = pd.read_csv(StringIO(response_obj['Body'].read().decode('utf-8')))
                
                # Filtrar dados (SP e Nacional)
                df_filtrado = df[
                    (df['Sigla_Estado'] == 'SP') | 
                    (df['Tipo_Feriado'].str.contains('NACIONAL', na=False, case=False))
                ]
                
                # Salvar arquivo individual no bucket destino
                nome_arquivo = chave_origem.replace('feriados-raw/', 'feriados-processados/')
                csv_content = df_filtrado.to_csv(index=False)
                
                s3_client.put_object(
                    Bucket=BUCKET_DESTINO,
                    Key=nome_arquivo,
                    Body=csv_content,
                    ContentType='text/csv',
                    Metadata={
                        'origem': BUCKET_ORIGEM,
                        'processado_em': datetime.now().isoformat(),
                        'registros_originais': str(len(df)),
                        'registros_filtrados': str(len(df_filtrado))
                    }
                )
                
                # Adicionar à consolidação
                todos_dataframes.append(df_filtrado)
                
                relatorio['arquivos_processados'].append({
                    'origem': chave_origem,
                    'destino': nome_arquivo,
                    'registros_originais': len(df),
                    'registros_filtrados': len(df_filtrado)
                })
                relatorio['transferidos'] += 1
                
                logger.info(f"✅ Transferido: {nome_arquivo} ({len(df)} → {len(df_filtrado)} registros)")
                
            except Exception as e:
                logger.error(f"❌ Erro processando {chave_origem}: {str(e)}")
                relatorio['erros'] += 1
        
        # Criar arquivo consolidado
        if todos_dataframes:
            logger.info("📋 Criando arquivo consolidado")
            df_consolidado = pd.concat(todos_dataframes, ignore_index=True)
            
            # Salvar consolidado
            chave_consolidado = f"feriados-processados/feriados_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_consolidado = df_consolidado.to_csv(index=False)
            
            s3_client.put_object(
                Bucket=BUCKET_DESTINO,
                Key=chave_consolidado,
                Body=csv_consolidado,
                ContentType='text/csv',
                Metadata={
                    'tipo': 'consolidado',
                    'total_registros': str(len(df_consolidado)),
                    'processado_em': datetime.now().isoformat()
                }
            )
            
            relatorio['arquivo_consolidado'] = chave_consolidado
            relatorio['total_registros_consolidado'] = len(df_consolidado)
            
            logger.info(f"✅ Consolidado criado: {chave_consolidado} ({len(df_consolidado)} registros)")
    
    except Exception as e:
        logger.error(f"❌ Erro geral: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    
    # Resposta
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Transferência concluída',
            'relatorio': relatorio
        }, indent=2)
    }