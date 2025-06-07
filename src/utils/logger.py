"""
Configuração de logging para o projeto.
"""
import os
import sys
import time
from loguru import logger


def setup_logger(log_dir: str = None, level: str = "INFO"):
    """
    Configura o logger para o projeto.

    Args:
        log_dir: Diretório para os arquivos de log. Se None, usa logs/ na raiz.
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    # Remove handlers padrão
    logger.remove()
    
    # Configura log para o console
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=level,
        colorize=True
    )
    
    # Configura log para arquivo
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"etl_{time.strftime('%Y%m%d_%H%M%S')}.log")
        
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level=level,
            rotation="10 MB",  # Rotaciona quando o arquivo atingir 10 MB
            retention="1 month",  # Mantém logs por 1 mês
            compression="zip"  # Comprime logs antigos
        )
        
        logger.info(f"Log configurado em: {log_file}")
    
    return logger


def get_logger():
    """
    Obtém a instância configurada do logger.

    Returns:
        Instância do logger.
    """
    return logger