"""
Exemplo de teste para as funções utilitárias
"""

import unittest
import pandas as pd
import tempfile
import os
from src.utils.data_utils import verificar_estrutura_diretorios, gerar_relatorio_dados

class TestDataUtils(unittest.TestCase):
    
    def test_gerar_relatorio_dados(self):
        """Testa geração de relatório de dados"""
        # Criar DataFrame de teste
        df_teste = pd.DataFrame({
            'coluna1': [1, 2, 3, None],
            'coluna2': ['A', 'B', None, 'D']
        })
        
        relatorio = gerar_relatorio_dados(df_teste, "teste")
        
        self.assertEqual(relatorio['registros'], 4)
        self.assertEqual(relatorio['colunas'], 2)
        self.assertEqual(relatorio['valores_ausentes'], 2)
        self.assertIn('dataset', relatorio)
    
    def test_relatorio_dataframe_vazio(self):
        """Testa relatório com DataFrame vazio"""
        df_vazio = pd.DataFrame()
        relatorio = gerar_relatorio_dados(df_vazio, "vazio")
        
        self.assertIn('erro', relatorio)

if __name__ == '__main__':
    unittest.main()