"""
Módulo de inicialização para os extratores, incluindo a fábrica de extratores.
"""
from typing import Dict, Any, Optional
from loguru import logger

from src.utils.config_loader import ConfigLoader
from src.etl.extractors.base_extractor import BaseExtractor
from src.etl.extractors.btg_extractor import BTGExtractor
from src.etl.extractors.daycoval_extractor import DaycovalExtractor
from src.etl.extractors.master_extractor import MasterExtractor
from src.etl.extractors.singulare_extractor import SingulareExtractor


class ExtractorFactory:
    """
    Fábrica para criar instâncias de extratores com base no tipo de custodiante.
    
    Implementa o padrão Factory Method.
    """
    
    def __init__(self, config_loader: ConfigLoader = None):
        """
        Inicializa a fábrica de extratores.
        
        Args:
            config_loader: Instância do carregador de configurações.
        """
        self.config_loader = config_loader or ConfigLoader()
        
        # Mapeamento de custodiantes para classes extratoras
        self.extractor_map = {
            "BTG": BTGExtractor,
            "Daycoval": DaycovalExtractor,
            "Master": MasterExtractor,
            "Singulare": SingulareExtractor
        }
    
    def get_extractor(self, custodiante: str) -> Optional[BaseExtractor]:
        """
        Obtém um extrator específico para o custodiante.
        
        Args:
            custodiante: Nome do custodiante.
            
        Returns:
            Instância do extrator ou None se não houver extrator para o custodiante.
        """
        # Normaliza o nome do custodiante
        custodiante = custodiante.strip().title()
        
        # Verificar se existe extrator para o custodiante
        if custodiante not in self.extractor_map:
            logger.warning(f"Extrator não encontrado para o custodiante: {custodiante}")
            return None
        
        # Criar e retornar a instância do extrator
        extractor_class = self.extractor_map[custodiante]
        return extractor_class(self.config_loader)
    
    def create_all_extractors(self) -> Dict[str, BaseExtractor]:
        """
        Cria instâncias de todos os extratores disponíveis.
        
        Returns:
            Dicionário com as instâncias de extratores, mapeadas por nome de custodiante.
        """
        extractors = {}
        
        for custodiante, extractor_class in self.extractor_map.items():
            try:
                extractors[custodiante] = extractor_class(self.config_loader)
            except Exception as e:
                logger.error(f"Erro ao criar extrator para {custodiante}: {e}")
        
        return extractors