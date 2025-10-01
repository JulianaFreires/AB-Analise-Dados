# 📊 Análise de Dados - IPCA e Feriados

Este projeto realiza análises de dados do IPCA (IBGE) e feriados brasileiros, seguindo princípios de Clean Architecture.

## 🏗️ Estrutura do Projeto

```
analise-dados/
├── 📁 data/                    # Camada de Dados
│   ├── raw/                    # Dados brutos (fonte original)
│   ├── processed/              # Dados processados/limpos
│   └── external/               # Dados externos (APIs, web scraping)
│
├── 📁 src/                     # Código Fonte
│   ├── notebooks/              # Jupyter Notebooks para análise
│   ├── scripts/                # Scripts Python executáveis
│   └── utils/                  # Utilitários e funções auxiliares
│
├── 📁 docs/                    # Documentação
├── 📁 reports/                 # Relatórios gerados
├── 📁 tests/                   # Testes automatizados
├── 📁 config/                  # Configurações do projeto
└── README.md                   # Este arquivo
```

## 📦 Componentes

### 🔹 Dados (data/)
- **raw/**: Arquivos originais do IBGE (br_ibge_ipca_mes_brasil.csv.gz)
- **processed/**: Dados tratados e limpos (ipca_tratado.csv)
- **external/**: Dados de feriados obtidos via web scraping

### 🔹 Código (src/)
- **notebooks/**: Análises interativas em Jupyter
  - `tratamento_ipca.ipynb` - Processamento dos dados IPCA
  - `feriados_simples.ipynb` - Análise de feriados brasileiros
- **scripts/**: Scripts para processamento
  - `tratamento_ipca.py` - Script de tratamento IPCA
  - `feriados.py` - Web scraper de feriados
  - `feriados_basico.py` - Versão simplificada

## 🚀 Como Usar

### 1. Análise IPCA
```bash
# Via notebook (recomendado)
jupyter notebook src/notebooks/tratamento_ipca.ipynb

# Via script
python src/scripts/tratamento_ipca.py
```

### 2. Análise Feriados
```bash
# Via notebook
jupyter notebook src/notebooks/feriados_simples.ipynb

# Via script
python src/scripts/feriados.py
```

## 📋 Dependências

```bash
pip install pandas requests gzip
```

## 🔄 Fluxo de Dados

```
data/raw/           →  src/scripts/       →  data/processed/
(dados originais)      (processamento)       (dados limpos)
        ↓                     ↓                    ↓
data/external/      →  src/notebooks/     →  reports/
(dados externos)       (análises)           (relatórios)
```

## 📊 Principais Funcionalidades

- ✅ Processamento de dados IPCA do IBGE
- ✅ Web scraping de feriados brasileiros
- ✅ Limpeza e padronização de dados
- ✅ Análises exploratórias
- ✅ Geração de relatórios

## 📝 Notas

- Projeto segue princípios de Clean Architecture
- Separação clara entre dados, código e documentação
- Notebooks para análise interativa
- Scripts para automação