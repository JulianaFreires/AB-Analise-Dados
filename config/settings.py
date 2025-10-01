# Configurações do Projeto

# Caminhos dos dados
DATA_RAW_PATH = "data/raw/"
DATA_PROCESSED_PATH = "data/processed/"
DATA_EXTERNAL_PATH = "data/external/"

# Arquivos principais
IPCA_RAW_FILE = "br_ibge_ipca_mes_brasil.csv.gz"
IPCA_PROCESSED_FILE = "ipca_processado.csv"

# URLs para feriados
FERIADOS_BASE_URL = "https://raw.githubusercontent.com/gabriel-milan/feriados-brasileiros/main/"
FERIADOS_ANOS = [2024, 2025]
FERIADOS_TIPOS = ["nacional", "estadual", "municipal", "facultativo"]

# Configurações de encoding
DEFAULT_ENCODING = "utf-8"

# Estados de interesse (pode ser expandido)
ESTADOS_FOCO = ["SP", "RJ", "MG", "RS"]