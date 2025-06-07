"""
Módulo com o extrator específico para o custodiante Master.
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


class MasterExtractor(BaseExtractor):
    """Extrator específico para o custodiante Master."""

    def get_custodiante_name(self) -> str:
        """
        Retorna o nome do custodiante.

        Returns:
            Nome do custodiante.
        """
        return "Master"
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extrai dados de um arquivo do Master.

        Args:
            file_path: Caminho do arquivo.

        Returns:
            DataFrame com os dados extraídos.
        """
        if not self.validate_file(file_path):
            return pd.DataFrame()
        
        try:
            # Lê o arquivo CSV
            df = pd.read_csv(
                file_path,
                sep=';',  # Delimitador padrão para arquivos do Master
                encoding='latin1',
                on_bad_lines='warn'
            )
            
            # Pré-processamento e validações
            df = self.preprocess_dataframe(df)
            df = self.apply_validations(df)
            
            # Normalizar dados
            df = self._normalize_master_data(df, file_path)
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do arquivo {file_path}: {e}")
            raise

    def _normalize_master_data(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """
        Normaliza os dados do Master para o formato padrão.

        Args:
            df: DataFrame com dados brutos.
            file_path: Caminho do arquivo (para extrair data se necessário).

        Returns:
            DataFrame normalizado.
        """
        # Cria cópia para evitar SettingWithCopyWarning
        df = df.copy()
        logger.info(f"Iniciando normalização de dados Master. Registros: {len(df)}")
        
        # Renomear colunas para o padrão
        column_map = {
            'carteira': 'nome_fundo',
            'datalancamento': 'data',
            'dataliquidacao': 'data_liquidacao',
            'historico': 'lancamento',
            'credito': 'valor_credito',
            'debito': 'valor_debito',
            'saldo': 'saldo',
            'codigolancamento': 'codigo_lancamento'
        }
        
        # Renomear colunas
        for old_col, new_col in column_map.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
        
        # Função auxiliar para converter valores monetários
        def convert_monetary_value(value):
            if pd.isna(value) or value == "":
                return 0.0
            
            # Converte para string para garantir
            value_str = str(value).strip()
            
            # Verifica se é um valor entre parênteses (negativo)
            is_negative = '(' in value_str and ')' in value_str
            
            # Remove parênteses se presentes
            value_str = value_str.replace('(', '').replace(')', '')
            
            # Substitui vírgula por ponto para conversão decimal
            value_str = value_str.replace('.', '').replace(',', '.')
            
            try:
                # Converte para float
                value_float = float(value_str)
                
                # Aplica sinal negativo se estava entre parênteses
                if is_negative:
                    value_float = -value_float
                    
                return value_float
            except ValueError:
                logger.warning(f"Não foi possível converter o valor '{value}' para número")
                return 0.0
        
        # Converter valores para float - tratando corretamente valores com parênteses
        df['valor_credito'] = df['valor_credito'].apply(convert_monetary_value)
        df['valor_debito'] = df['valor_debito'].apply(convert_monetary_value)
        
        # Lógica para determinar valor final e tipo de lançamento
        df['valor'] = 0.0  # Inicializa com zero
        df['tipo_lancamento'] = 'Crédito'  # Valor padrão
        
        # Verifica se há valor de crédito
        mask_credito = df['valor_credito'].notna() & (df['valor_credito'] != 0)
        df.loc[mask_credito, 'valor'] = df.loc[mask_credito, 'valor_credito']
        
        # Verifica se há valor de débito (prioriza débito se ambos estiverem presentes)
        mask_debito = df['valor_debito'].notna() & (df['valor_debito'] != 0)
        df.loc[mask_debito, 'valor'] = df.loc[mask_debito, 'valor_debito'].abs()  # Use abs para garantir valor positivo
        df.loc[mask_debito, 'tipo_lancamento'] = 'Débito'
        
        # Converter data para formato padrão
        if 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
        
        # Selecionar e reorganizar colunas
        cols = [
            'data', 
            'nome_fundo', 
            'lancamento', 
            'valor', 
            'tipo_lancamento', 
            'saldo',
            'codigo_lancamento'
        ]
        
        # Garantir que todas as colunas existam
        for col in cols:
            if col not in df.columns:
                df[col] = None
        
        final_df = df[cols].copy()
        
        # Verificação final - validando que há valores
        nao_zeros = (final_df['valor'] != 0).sum()
        total = len(final_df)
        logger.info(f"Normalização concluída. Total de registros: {total}, com valores não-zero: {nao_zeros} ({nao_zeros/total*100:.1f}%)")
        
        # Log para depuração - mostra alguns exemplos de valores
        log_sample = final_df.head(5)
        logger.debug(f"Amostra de valores processados:\n{log_sample[['data', 'lancamento', 'valor', 'tipo_lancamento']]}")
        
        return final_df