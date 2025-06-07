"""
Módulo com o extrator específico para o custodiante Daycoval.
"""
import pandas as pd
import numpy as np
import os
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger
import re
from datetime import datetime

from src.etl.extractors.base_extractor import BaseExtractor
from src.utils.file_utils import read_csv_with_config, clean_and_normalize_dataframe


class DaycovalExtractor(BaseExtractor):
    """Extrator específico para o custodiante Daycoval."""

    def get_custodiante_name(self) -> str:
        """
        Retorna o nome do custodiante.

        Returns:
            Nome do custodiante.
        """
        return "Daycoval"
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extrai dados de um arquivo do Daycoval.

        Args:
            file_path: Caminho do arquivo.

        Returns:
            DataFrame com os dados extraídos.
        """
        if not self.validate_file(file_path):
            return pd.DataFrame()
        
        try:
            # Leitura inicial para detecção de padrões
            encoding = self.config.get('encoding', 'latin1')
            separator = self.config.get('separator', ';')
            
            # Estratégia especial para o Daycoval que tem estrutura complexa
            df = self._read_daycoval_file(file_path, encoding, separator)
            
            if df.empty:
                logger.warning(f"Nenhum dado extraído do arquivo: {file_path}")
                return pd.DataFrame()
            
            # Pré-processamento e validações
            df = self.preprocess_dataframe(df)
            df = self.apply_validations(df)
            
            # Normalizar e transformar dados
            df = self._normalize_daycoval_data(df, file_path)
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do arquivo {file_path}: {e}")
            raise
    
    def _read_daycoval_file(self, file_path: str, encoding: str, separator: str) -> pd.DataFrame:
        """
        Lê um arquivo específico do Daycoval, tratando sua estrutura particular.
        """
        # Primeiro determinar o tipo de arquivo
        file_extension = os.path.splitext(file_path)[1].lower()
        
        if file_extension in ['.xlsx', '.xls']:
            try:
                # Para arquivos Excel, usar pandas read_excel
                return pd.read_excel(
                    file_path,
                    engine='openpyxl' if file_extension == '.xlsx' else 'xlrd'
                )
            except Exception as e:
                logger.error(f"Erro ao ler arquivo Excel {file_path}: {e}")
                return pd.DataFrame()
        
        # Para arquivos CSV, analisar o conteúdo para determinar o formato
        try:
            # Verificar se é um arquivo no formato específico do Demonstrativo de Caixa
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                first_lines = [next(f) for _ in range(10) if f]
            
            # Verificar padrão do Demonstrativo de Caixa
            if any('P1051Det' in line for line in first_lines):
                # Este é o formato específico do exemplo compartilhado
                result_rows = []
                
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    for line in f:
                        parts = line.strip().split(separator)
                        if len(parts) < 10:
                            continue
                            
                        if 'P1051Det' in parts[1]:
                            # Extrair dados
                            data = {}
                            # Obter índices de colunas importantes
                            data_idx = 4  # Normalmente a data está na 5ª coluna (índice 4)
                            fundo_idx = 7  # Nome do fundo
                            historico_idx = 9  # Histórico/lançamento
                            credito_idx = 10  # Crédito
                            debito_idx = 11  # Débito
                            
                            if len(parts) > data_idx:
                                data['data'] = parts[data_idx].strip()
                            if len(parts) > fundo_idx:
                                data['nome_fundo'] = parts[fundo_idx].strip()
                            if len(parts) > historico_idx:
                                data['lancamento'] = parts[historico_idx].strip()
                            if len(parts) > credito_idx:
                                data['valor_credito'] = parts[credito_idx].strip()
                            if len(parts) > debito_idx:
                                data['valor_debito'] = parts[debito_idx].strip()
                                
                            result_rows.append(data)
                
                if result_rows:
                    return pd.DataFrame(result_rows)
            
            # Tenta leitura padrão de CSV se não for o formato específico
            return pd.read_csv(
                file_path,
                sep=separator,
                encoding=encoding,
                on_bad_lines='skip',
                low_memory=False
            )
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {file_path}: {e}")
            return pd.DataFrame()

    
    def _normalize_daycoval_data(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """
        Normaliza os dados do Daycoval para o formato padrão.
        """
        # Cria cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Mapear colunas conforme necessário
        column_map = {
            'datalancamento': 'data',
            'historico': 'lancamento',
            'credito': 'valor_credito',
            'debito': 'valor_debito',
            'saldo': 'saldo',
            'carteira': 'nome_fundo',
            'nmfundo': 'nome_fundo',
            'complemento': 'observacao'
        }
        
        # Renomear colunas
        for old_col, new_col in column_map.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
        
        # Garantir que a coluna de data seja corretamente processada
        if 'data' in df.columns:
            # Primeiro remover espaços extras
            df['data'] = df['data'].astype(str).str.strip()
            
            # Tentar converter com diferentes formatos de data
            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                try:
                    temp_dates = pd.to_datetime(df['data'], format=fmt, errors='coerce')
                    # Se pelo menos 50% das datas foram convertidas com sucesso, usar este formato
                    if temp_dates.notna().mean() > 0.5:
                        df['data'] = temp_dates
                        break
                except:
                    continue
        
        # Normalizar valores
        for col in ['valor_credito', 'valor_debito']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '.').str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calcular valor final usando a lógica correta para créditos e débitos
        df['valor'] = df['valor_credito'].fillna(0)
        mask = df['valor_debito'].notna() & (df['valor_debito'] != 0)
        df.loc[mask, 'valor'] = df.loc[mask, 'valor_debito']
        
        # Identificar tipo de lançamento corretamente
        df['tipo_lancamento'] = 'Crédito'
        df.loc[mask, 'tipo_lancamento'] = 'Débito'
        
        # Garantir valores positivos
        df['valor'] = df['valor'].abs()
        
        # Selecionar e reorganizar colunas
        cols = ['data', 'nome_fundo', 'lancamento', 'valor', 'tipo_lancamento', 'observacao']
        
        # Garantir que todas as colunas existam
        for col in cols:
            if col not in df.columns:
                df[col] = None
        
        final_df = df[cols].copy()
        
        return final_df