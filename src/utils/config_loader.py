"""
Módulo para carregar e gerenciar configurações do projeto.
"""
import os
import json
from typing import Dict, Any, Optional, List, Tuple
from loguru import logger
import pandas as pd


class ConfigLoader:
    """Classe responsável por carregar e gerenciar configurações do projeto."""

    def __init__(self, config_dir: str = None):
        """
        Inicializa o carregador de configurações.

        Args:
            config_dir: Diretório de configurações. Se None, usa o diretório config/ na raiz.
        """
        if config_dir is None:
            # Assume que está sendo executado do diretório raiz do projeto
            self.config_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                "config"
            )
        else:
            self.config_dir = config_dir
        
        self.configs = {}
        self.fidc_to_fics = {}
        self.fic_to_fidc = {}
    
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """
        Carrega um arquivo de configuração específico.

        Args:
            config_name: Nome do arquivo de configuração (sem extensão).

        Returns:
            Dicionário com as configurações carregadas.
        """
        config_path = os.path.join(self.config_dir, f"{config_name}.json")
        
        if not os.path.exists(config_path):
            logger.error(f"Arquivo de configuração não encontrado: {config_path}")
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.configs[config_name] = config
                return config
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON do arquivo {config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo de configuração {config_path}: {e}")
            raise
    
    def get_config(self, config_name: str) -> Dict[str, Any]:
        """
        Obtém uma configuração já carregada ou carrega-a caso não esteja.

        Args:
            config_name: Nome da configuração.

        Returns:
            Dicionário com as configurações.
        """
        if config_name not in self.configs:
            return self.load_config(config_name)
        return self.configs[config_name]
    
    def get_custodiante_config(self, custodiante: str) -> Optional[Dict[str, Any]]:
        """
        Obtém a configuração específica para um custodiante.

        Args:
            custodiante: Nome do custodiante.

        Returns:
            Configuração do custodiante ou None se não encontrada.
        """
        map_files = self.get_config("map_files")
        
        if custodiante in map_files.get("custodiantes", {}):
            return map_files["custodiantes"][custodiante]
        
        logger.warning(f"Configuração não encontrada para o custodiante: {custodiante}")
        return None
    
    def get_lancamentos_map(self) -> Dict[str, str]:
        """
        Obtém o mapeamento de padronização de lançamentos.

        Returns:
            Dicionário com o mapeamento de lançamentos.
        """
        return self.get_config("map_lancamentos")
    
    def load_fic_fidc_mapping(self) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
        """
        Carrega o mapeamento entre FICs e FIDCs.
        
        Returns:
            Tupla contendo dois dicionários:
            - fidc_to_fics: Mapeamento de FIDC para uma lista de FICs
            - fic_to_fidc: Mapeamento de FIC para FIDC
        """
        # Se já carregou, retorna os existentes
        if self.fidc_to_fics and self.fic_to_fidc:
            return self.fidc_to_fics, self.fic_to_fidc
        
        try:
            # Carregar o mapeamento
            mapping_data = self.get_config("fic_fidc_mapping")
            
            # Extrair os dicionários
            self.fidc_to_fics = mapping_data.get("fidc_to_fics", {})
            self.fic_to_fidc = mapping_data.get("fic_to_fidc", {})
            
            logger.info(f"Mapeamento FIC-FIDC carregado: {len(self.fidc_to_fics)} FIDCs e {len(self.fic_to_fidc)} FICs")
            return self.fidc_to_fics, self.fic_to_fidc
            
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento FIC-FIDC: {e}")
            return {}, {}
    
    def get_categorized_fund_name(self, fund_name: str) -> str:
        """
        Obtém o nome categorizado do fundo com base no mapeamento FIC-FIDC.
        """
        if not fund_name or pd.isna(fund_name):
            return fund_name
        
        # Carregar o mapeamento se ainda não foi carregado
        if not self.fidc_to_fics or not self.fic_to_fidc:
            self.load_fic_fidc_mapping()
        
        # Normalizar nome - remover acentos, converter para maiúsculas
        import unicodedata
        
        def normalize_string(s):
            if not isinstance(s, str):
                return str(s)
            s = s.strip().upper()
            # Remover acentos
            s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
            return s
        
        norm_name = normalize_string(fund_name)
        
        # Normalizar também as chaves dos dicionários para correspondência
        normalized_fic_to_fidc = {normalize_string(k): v for k, v in self.fic_to_fidc.items()}
        normalized_fidc_to_fics = {normalize_string(k): v for k, v in self.fidc_to_fics.items()}
        
        # Verificar se é um FIC
        if norm_name in normalized_fic_to_fidc:
            return normalized_fic_to_fidc[norm_name]
        
        # Verificar correspondência parcial para FICs
        for fic, fidc in normalized_fic_to_fidc.items():
            if fic in norm_name:
                return fidc
        
        # Verificar se é um FIDC
        if norm_name in normalized_fidc_to_fics:
            return fund_name
        
        # Verificar correspondência parcial para FIDCs
        for fidc in normalized_fidc_to_fics:
            if fidc in norm_name:
                return fund_name
        
        # Se não encontrar, retornar o próprio nome
        return fund_name
    
    def get_despesas_classifications(self) -> List[str]:
        """
        Obtém a lista de lançamentos classificados como despesas.
        
        Returns:
            Lista de strings com os nomes dos lançamentos classificados como despesas.
        """
        try:
            config = self.get_config("despesas_classificacao")
            return config.get("lancamentos_despesa", [])
        except Exception as e:
            logger.error(f"Erro ao carregar classificações de despesas: {e}")
            # Lista padrão caso o arquivo não seja encontrado
            return [
                "Grafeno",
                "Taxa de Administração",
                "Anbima",
                "Taxa Anbima",
                "Taxa de Gestão",
                "Taxa de Custódia",
                "Despesas",
                "Auditoria",
                "Custódia",
                "Tarifa Bancária"
            ]