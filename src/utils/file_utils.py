"""
Utilitários para manipulação de arquivos.
"""
import os
import pandas as pd
import glob
from typing import List, Dict, Any, Union, Optional
from loguru import logger
import re
from datetime import datetime


def list_files(
    directory: str, 
    pattern: str = "*.*", 
    recursive: bool = False
) -> List[str]:
    """
    Lista arquivos em um diretório que correspondem a um padrão.

    Args:
        directory: Diretório a ser pesquisado.
        pattern: Padrão para filtrar arquivos (glob).
        recursive: Se deve pesquisar subdiretórios.

    Returns:
        Lista de caminhos de arquivos encontrados.
    """
    search_pattern = os.path.join(directory, pattern)
    
    if recursive:
        search_pattern = os.path.join(directory, "**", pattern)
        return glob.glob(search_pattern, recursive=True)
    
    return glob.glob(search_pattern)


def detect_custodiante(filename: str) -> Optional[str]:
    """
    Detecta o custodiante com base no nome do arquivo.

    Args:
        filename: Nome do arquivo.

    Returns:
        Nome do custodiante detectado ou None se não for possível detectar.
    """
    filename_lower = filename.lower()
    
    if "cashstatement" in filename_lower:
        return "Singulare"
    elif "ptr_" in filename_lower:
        return "Master"
    elif "demonstrativo de caixa" in filename_lower:
        return "Daycoval"
    elif "caixaextrato" in filename_lower:
        return "BTG"
    
    # Se não for possível detectar automaticamente
    logger.warning(f"Não foi possível detectar o custodiante para o arquivo: {filename}")
    return None


def read_csv_with_config(
    file_path: str, 
    config: Dict[str, Any]
) -> pd.DataFrame:
    """
    Lê um arquivo CSV com base na configuração fornecida.

    Args:
        file_path: Caminho do arquivo CSV.
        config: Configuração para leitura do arquivo.

    Returns:
        DataFrame com os dados lidos.
    """
    encoding = config.get("encoding", "utf-8")
    separator = config.get("separator", ";")
    skip_rows = config.get("skip_rows", 0)
    
    try:
        # Tentativa inicial com parâmetros básicos
        df = pd.read_csv(
            file_path,
            sep=separator,
            skiprows=skip_rows,
            encoding=encoding,
            error_bad_lines=False,  # Ignora linhas com problemas
            warn_bad_lines=True,    # Avisa sobre linhas ignoradas
            low_memory=False        # Evita problemas com tipos de dados mistos
        )
        return df
    except Exception as e:
        logger.error(f"Erro ao ler arquivo CSV {file_path}: {e}")
        
        # Estratégia de fallback: tentar ler com diferentes encoding
        fallback_encodings = ["latin1", "ISO-8859-1", "cp1252"]
        for enc in fallback_encodings:
            if enc != encoding:
                try:
                    logger.info(f"Tentando ler com encoding alternativo: {enc}")
                    df = pd.read_csv(
                        file_path,
                        sep=separator,
                        skiprows=skip_rows,
                        encoding=enc,
                        error_bad_lines=False,
                        warn_bad_lines=True,
                        low_memory=False
                    )
                    return df
                except Exception:
                    continue
        
        # Se todas as tentativas falharem
        raise ValueError(f"Não foi possível ler o arquivo CSV: {file_path}")


def detect_header_in_file(
    file_path: str, 
    max_lines: int = 30, 
    encoding: str = "utf-8"
) -> int:
    """
    Detecta a linha de cabeçalho em um arquivo.

    Args:
        file_path: Caminho do arquivo.
        max_lines: Número máximo de linhas para verificar.
        encoding: Encoding do arquivo.

    Returns:
        Número da linha onde o cabeçalho foi detectado.
    """
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            lines = [f.readline().strip() for _ in range(max_lines)]
        
        # Expressões regulares para identificar possíveis cabeçalhos
        header_patterns = [
            r'^(?:Data|DATA|date|DT)(?:;|,|\t)',
            r'^(?:ID|id|Id|CODIGO|codigo|Codigo)(?:;|,|\t)',
            r'^(?:CARTEIRA|Carteira|carteira)(?:;|,|\t)',
            r'(?:;|,|\t)(?:Valor|VALOR|valor)(?:;|,|\t)',
            r'(?:;|,|\t)(?:Saldo|SALDO|saldo)(?:;|,|\t)'
        ]
        
        for i, line in enumerate(lines):
            for pattern in header_patterns:
                if re.search(pattern, line):
                    return i
        
        # Se não encontrar um cabeçalho claro, retorna 0
        return 0
    
    except Exception as e:
        logger.error(f"Erro ao detectar cabeçalho no arquivo {file_path}: {e}")
        return 0


def clean_and_normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e normaliza um DataFrame, removendo espaços extras, valores NaN, etc.

    Args:
        df: DataFrame a ser limpo.

    Returns:
        DataFrame limpo e normalizado.
    """
    # Cria uma cópia para evitar SettingWithCopyWarning
    df = df.copy()
    
    # Normaliza nomes de colunas
    df.columns = [str(col).strip().lower() for col in df.columns]
    
    # Remove linhas completamente vazias
    df = df.dropna(how='all')
    
    # Para colunas de texto, remove espaços extras
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    return df


def save_dataframe(
    df: pd.DataFrame, 
    output_path: str, 
    format: str = "csv",
    index: bool = False
) -> None:
    """
    Salva um DataFrame em um arquivo.

    Args:
        df: DataFrame a ser salvo.
        output_path: Caminho do arquivo de saída.
        format: Formato de saída ('csv', 'excel', 'parquet').
        index: Se deve incluir o índice.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        if format.lower() == 'csv':
            df.to_csv(output_path, index=index, encoding='utf-8')
        elif format.lower() == 'excel':
            df.to_excel(output_path, index=index, engine='openpyxl')
        elif format.lower() == 'parquet':
            df.to_parquet(output_path, index=index)
        else:
            raise ValueError(f"Formato não suportado: {format}")
        
        logger.info(f"DataFrame salvo com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame em {output_path}: {e}")
        raise


def get_date_from_filename(filename: str) -> Optional[datetime]:
    """
    Extrai a data do nome do arquivo.

    Args:
        filename: Nome do arquivo.

    Returns:
        Data extraída ou None se não for possível extrair.
    """
    # Padrões comuns de data em nomes de arquivos
    patterns = [
        r'(\d{2})[-_](\d{2})[-_](\d{4})',  # DD-MM-YYYY ou DD_MM_YYYY
        r'(\d{4})[-_](\d{2})[-_](\d{2})',  # YYYY-MM-DD ou YYYY_MM_DD
        r'(\d{2})(\d{2})(\d{4})',          # DDMMYYYY
        r'(\d{4})(\d{2})(\d{2})',          # YYYYMMDD
    ]
    
    basename = os.path.basename(filename)
    
    for pattern in patterns:
        match = re.search(pattern, basename)
        if match:
            groups = match.groups()
            if len(groups[0]) == 4:  # YYYY-MM-DD
                year, month, day = groups
            else:  # DD-MM-YYYY
                day, month, year = groups
            
            try:
                return datetime(int(year), int(month), int(day))
            except ValueError:
                continue
    
    return None