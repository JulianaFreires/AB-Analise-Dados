# 🚀 Função Lambda - Processador de Feriados para S3

## 📋 Descrição
Função AWS Lambda que processa feriados brasileiros e salva diretamente em bucket S3.

## 🔧 Configuração da Lambda

### Variáveis de Ambiente:
```
S3_BUCKET_NAME = nome-do-seu-bucket
```

### Configurações Recomendadas:
- **Runtime**: Python 3.9 ou superior
- **Timeout**: 5-10 minutos
- **Memória**: 512MB - 1GB
- **Handler**: `lambda_function.lambda_handler`

### Permissões IAM Necessárias:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::seu-bucket/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream", 
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

## 📁 Estrutura de Arquivos Gerados no S3

```
seu-bucket/
├── feriados/
│   ├── nacional_2024.csv
│   ├── nacional_2025.csv
│   ├── nacional_2026.csv
│   ├── estadual_2024.csv
│   ├── estadual_2025.csv
│   ├── estadual_2026.csv
│   ├── municipal_2024.csv
│   ├── municipal_2025.csv
│   ├── municipal_2026.csv
│   ├── facultativo_2024.csv
│   ├── facultativo_2025.csv
│   ├── facultativo_2026.csv
│   └── feriados_completo_YYYYMMDD.csv
```

## 🔄 Funcionalidades

1. **Download Automático**: Baixa dados de feriados do GitHub
2. **Processamento**: Padroniza colunas e formatos
3. **Filtragem**: Foca em feriados de SP e nacionais
4. **Upload S3**: Salva arquivos individuais e consolidado
5. **Logs**: Registro detalhado do processamento
6. **Relatório**: Retorna status e estatísticas

## 📊 Dados Processados

- **Feriados Nacionais**: Válidos para todo o Brasil
- **Feriados Estaduais**: Focado no estado de SP
- **Feriados Municipais**: Municípios de SP
- **Feriados Facultativos**: Opcionais

## 🚀 Como Usar

1. **Criar bucket S3**
2. **Fazer upload do código**
3. **Configurar variável de ambiente**
4. **Configurar permissões IAM**
5. **Executar manualmente ou via trigger**

## 🔄 Triggers Sugeridos

- **EventBridge**: Execução programada (ex: mensalmente)
- **S3 Event**: Quando novos dados chegam
- **API Gateway**: Execução via HTTP

## 📈 Monitoramento

- **CloudWatch Logs**: Logs detalhados
- **CloudWatch Metrics**: Duração e erros
- **X-Ray**: Tracing (se habilitado)

## ✅ Resposta de Sucesso
```json
{
  "statusCode": 200,
  "body": {
    "message": "Processamento concluído com sucesso",
    "bucket": "seu-bucket",
    "relatorio": {
      "processados": 12,
      "erros": 0,
      "arquivos_salvos": ["feriados/nacional_2024.csv", "..."],
      "total_registros": 150,
      "timestamp": "2025-10-01T17:30:00"
    }
  }
}
```