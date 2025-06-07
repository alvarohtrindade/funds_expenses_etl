"""
Módulo com a classe base para extratores de dados.
"""
import os
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from loguru import logger

from src.utils.config_loader import ConfigLoader


class BaseExtractor(ABC):
    """Classe base para todos os extratores de dados."""

    def __init__(self, config_loader: ConfigLoader = None):
        """
        Inicializa o extrator.

        Args:
            config_loader: Instância do carregador de configurações.
        """
        self.config_loader = config_loader or ConfigLoader()
        self.custodiante = self.get_custodiante_name()
        self.config = self.config_loader.get_custodiante_config(self.custodiante)
        
        if not self.config:
            logger.warning(f"Configuração não encontrada para o custodiante: {self.custodiante}")
    
    @abstractmethod
    def get_custodiante_name(self) -> str:
        """
        Retorna o nome do custodiante.

        Returns:
            Nome do custodiante.
        """
        pass
    
    @abstractmethod
    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extrai dados de um arquivo.

        Args:
            file_path: Caminho do arquivo.

        Returns:
            DataFrame com os dados extraídos.
        """
        pass
    
    def validate_file(self, file_path: str) -> bool:
        """
        Valida se um arquivo pode ser processado por este extrator.

        Args:
            file_path: Caminho do arquivo.

        Returns:
            True se o arquivo for válido, False caso contrário.
        """
        if not os.path.exists(file_path):
            logger.error(f"Arquivo não encontrado: {file_path}")
            return False
            
        # Verificação básica de extensão
        _, ext = os.path.splitext(file_path)
        valid_extensions = ['.csv', '.xlsx', '.xls']
        
        if ext.lower() not in valid_extensions:
            logger.warning(f"Arquivo com extensão não suportada: {file_path}")
            return False
            
        return True
    
    def preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza pré-processamento comum a todos os extratores.

        Args:
            df: DataFrame original.

        Returns:
            DataFrame pré-processado.
        """
        # Cria uma cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Normaliza nomes das colunas
        df.columns = [str(col).strip().lower() for col in df.columns]
        
        # Remove linhas completamente vazias
        df = df.dropna(how='all')
        
        # Remove linhas duplicadas
        df = df.drop_duplicates()
        
        return df
    
    def apply_validations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica validações definidas na configuração.

        Args:
            df: DataFrame a ser validado.

        Returns:
            DataFrame validado.
        """
        if not self.config or 'validations' not in self.config:
            return df
            
        validations = self.config['validations']
        
        # Filtrar linhas onde colunas obrigatórias estão vazias
        if 'drop_if_empty' in validations:
            for col in validations['drop_if_empty']:
                if col in df.columns:
                    df = df.dropna(subset=[col])
        
        # Filtrar tipos específicos de lançamentos
        if 'filter_out_lancamentos' in validations:
            for lancamento in validations['filter_out_lancamentos']:
                # Busca em todas as colunas de texto por esse lançamento
                for col in df.select_dtypes(include=['object']).columns:
                    df = df[~df[col].str.contains(lancamento, na=False, case=False)]
        
        return df
    
    def extract_batch(self, file_paths: List[str]) -> pd.DataFrame:
        """
        Extrai dados de vários arquivos e concatena os resultados.

        Args:
            file_paths: Lista de caminhos de arquivos.

        Returns:
            DataFrame com todos os dados extraídos.
        """
        dataframes = []
        
        for file_path in file_paths:
            try:
                if self.validate_file(file_path):
                    df = self.extract(file_path)
                    if not df.empty:
                        dataframes.append(df)
                    else:
                        logger.warning(f"DataFrame vazio extraído de: {file_path}")
            except Exception as e:
                logger.error(f"Erro ao extrair dados do arquivo {file_path}: {e}")
                continue
        
        if not dataframes:
            logger.warning("Nenhum DataFrame válido foi extraído.")
            return pd.DataFrame()
            
        return pd.concat(dataframes, ignore_index=True)