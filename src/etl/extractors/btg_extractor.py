"""
Módulo com o extrator específico para o custodiante BTG.
"""
import pandas as pd
import numpy as np
import os
from typing import Dict, List, Any, Optional
from loguru import logger
import re
from datetime import datetime

from src.etl.extractors.base_extractor import BaseExtractor
from src.utils.file_utils import clean_and_normalize_dataframe


class BTGExtractor(BaseExtractor):
    """Extrator específico para o custodiante BTG."""

    def get_custodiante_name(self) -> str:
        """
        Retorna o nome do custodiante.

        Returns:
            Nome do custodiante.
        """
        return "BTG"
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extrai dados de um arquivo do BTG.

        Args:
            file_path: Caminho do arquivo.

        Returns:
            DataFrame com os dados extraídos.
        """
        if not self.validate_file(file_path):
            return pd.DataFrame()
        
        try:
            # Para arquivos Excel (BTG)
            if file_path.lower().endswith(('.xlsx', '.xls')):
                # Identificar as configurações específicas para o BTG
                skip_rows = self.config.get('skip_rows', 1)
                
                # Ler arquivo Excel
                df = pd.read_excel(
                    file_path,
                    skiprows=skip_rows,
                    engine='openpyxl' if file_path.lower().endswith('.xlsx') else 'xlrd'
                )
            else:
                # Fallback para CSV
                df = pd.read_csv(
                    file_path,
                    sep=';',
                    encoding='latin1',
                    on_bad_lines='warn'
                )
            
            # Pré-processamento e validações
            df = self.preprocess_dataframe(df)
            df = self.apply_validations(df)
            
            # Normalizar dados
            df = self._normalize_btg_data(df, file_path)
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do arquivo {file_path}: {e}")
            raise
    
    def _normalize_btg_data(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """
        Normaliza os dados do BTG para o formato padrão.

        Args:
            df: DataFrame com dados brutos.
            file_path: Caminho do arquivo (para extrair data se necessário).

        Returns:
            DataFrame normalizado.
        """
        # Cria cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Mapear colunas conforme configuração do BTG
        column_mapping = {
            'nome da classe': 'nome_fundo',
            'data': 'data',
            'lançamento': 'lancamento',
            'financeiro (r$)': 'valor',
            'saldo (r$)': 'saldo',
            'observação': 'observacao',
            'remetente': 'remetente'
        }
        
        # Normalizar nomes das colunas (para minúsculas)
        df.columns = [str(col).strip().lower() for col in df.columns]
        
        # Renomear colunas
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
        
        # Se não tiver a coluna valor, procurar alternativas
        if 'valor' not in df.columns:
            for col in df.columns:
                if 'financeiro' in col.lower() or 'valor' in col.lower():
                    df['valor'] = df[col]
                    break
        
        # Converter valor para float
        if 'valor' in df.columns:
            # Remover 'R e converter vírgula para ponto
            df['valor'] = df['valor'].astype(str).str.replace('R', '').str.replace(',', '.').str.strip()
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        # Identificar tipo de lançamento
        df['tipo_lancamento'] = np.where(df['valor'] >= 0, 'Crédito', 'Débito')
        
        # Garantir valores positivos
        df['valor'] = df['valor'].abs()
        
        # Converter data para formato padrão
        if 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'], errors='coerce')
        
        # Se a data não for reconhecida, tenta extrair do nome do arquivo
        if 'data' not in df.columns or df['data'].isna().all():
            date_match = re.search(r'(\d{2})[-_](\d{2})[-_](\d{4})', os.path.basename(file_path))
            if date_match:
                day, month, year = date_match.groups()
                date_str = f"{year}-{month}-{day}"
                df['data'] = pd.to_datetime(date_str)
        
        # Selecionar e reorganizar colunas
        cols = [
            'data', 
            'nome_fundo', 
            'lancamento', 
            'valor', 
            'tipo_lancamento', 
            'observacao',
            'remetente',
            'nmcategorizado'
        ]
        
        # Garantir que todas as colunas existam
        for col in cols:
            if col not in df.columns:
                df[col] = None
        
        final_df = df[cols].copy()
        
        # Aplicar outras normalizações necessárias
        print(final_df)
        final_df = clean_and_normalize_dataframe(final_df)
        print(final_df)
        
        return final_df