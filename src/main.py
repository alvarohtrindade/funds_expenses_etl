#!/usr/bin/env python3
"""
Módulo principal para CLI do ETL de despesas de fundos.
"""
import os
import sys
import click
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from loguru import logger
import traceback

from src.utils.logger import setup_logger
from src.utils.file_utils import list_files, detect_custodiante
from src.utils.config_loader import ConfigLoader

from src.etl.extractors.btg_extractor import BTGExtractor
from src.etl.extractors.daycoval_extractor import DaycovalExtractor
from src.etl.extractors.master_extractor import MasterExtractor
from src.etl.extractors.singulare_extractor import SingulareExtractor

from src.etl.transformers.transformer import ExpenseTransformer, DataFrameNormalizer
from src.etl.loaders.loader import DataLoader


@click.group()
def cli():
    """Ferramenta CLI para processamento de despesas de fundos."""
    pass


@cli.command()
@click.option('--file-path', '-f', required=True, help='Caminho do arquivo a ser processado')
@click.option('--output-dir', '-o', required=True, help='Diretório de saída')
@click.option('--custodiante', '-c', help='Custodiante (BTG, Daycoval, Master, Singulare)')
@click.option('--format', type=click.Choice(['csv', 'excel', 'parquet']), default='csv', help='Formato de saída')
@click.option('--log-dir', help='Diretório para logs')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), default='INFO', help='Nível de log')
@click.option('--to-mysql', is_flag=True, help='Salvar dados no banco MySQL (requer arquivo .env)')
@click.option('--mysql-table', default='despesas_fundos', help='Nome da tabela MySQL para salvar os dados')
@click.option('--truncate-table', is_flag=True, help='Limpar tabela MySQL antes de inserir novos dados')
@click.option('--env-path', default=None, help='Caminho para o arquivo .env com credenciais MySQL')
def process_file(
    file_path: str, 
    output_dir: str, 
    custodiante: Optional[str], 
    format: str,
    log_dir: Optional[str],
    log_level: str,
    to_mysql: bool,
    mysql_table: str,
    truncate_table: bool,
    env_path: Optional[str]
):
    """Processa um arquivo específico de despesas."""
    # Configurar logger
    logger_instance = setup_logger(log_dir or os.path.join(output_dir, "logs"), log_level)
    
    # Verificar se o arquivo existe
    if not os.path.exists(file_path):
        click.echo(f"Arquivo não encontrado: {file_path}")
        return
    
    # Detectar custodiante se não fornecido
    if custodiante is None:
        custodiante = detect_custodiante(file_path)
        if custodiante is None:
            click.echo(f"Não foi possível detectar o custodiante para o arquivo: {file_path}")
            click.echo("Por favor, especifique o custodiante usando a opção --custodiante")
            return
    
    # Inicializar componentes
    config_loader = ConfigLoader()
    
    # Selecionar o extrator apropriado
    extractor = None
    if custodiante == "BTG":
        extractor = BTGExtractor(config_loader)
    elif custodiante == "Daycoval":
        extractor = DaycovalExtractor(config_loader)
    elif custodiante == "Master":
        extractor = MasterExtractor(config_loader)
    elif custodiante == "Singulare":
        extractor = SingulareExtractor(config_loader)
    else:
        click.echo(f"Custodiante não suportado: {custodiante}")
        return
    
    try:
        # Extração
        logger.info(f"Processando arquivo {file_path} do custodiante {custodiante}")
        df = extractor.extract(file_path)
        
        if df.empty:
            logger.warning(f"Nenhum dado extraído do arquivo: {file_path}")
            click.echo(f"Nenhum dado extraído do arquivo: {file_path}")
            return
        
        # Transformação
        transformer = ExpenseTransformer(config_loader)
        df = transformer.transform(df)
        
        # Normalização adicional
        normalizer = DataFrameNormalizer()
        df = normalizer.normalize_columns(df)
        df = normalizer.remove_duplicates(df)
        df = normalizer.fill_missing_values(df)
        
        # Adicionar informações do custodiante
        df['custodiante'] = custodiante
        
        # Carregamento
        loader = DataLoader(output_dir)
        
        # Salvar em arquivo
        output_path = None
        if format == 'csv':
            output_path = loader.save_to_csv(df)
        elif format == 'excel':
            output_path = loader.save_to_excel(df)
        elif format == 'parquet':
            output_path = loader.save_to_parquet(df)
        
        logger.info(f"Arquivo {file_path} processado com sucesso: {len(df)} registros")
        click.echo(f"Arquivo {file_path} processado com sucesso: {len(df)} registros")
        
        # Salvar no MySQL, se solicitado
        if to_mysql:
            try:
                inserted = loader.save_to_mysql(
                    df,
                    table_name=mysql_table,
                    truncate=truncate_table,
                    env_path=env_path,
                )
                if inserted > 0:
                    click.echo(f"Dados inseridos no MySQL com sucesso: {inserted} registros.")
                else:
                    click.echo("Falha ao inserir dados no MySQL.")
            except Exception as e:
                click.echo(f"Erro ao salvar no MySQL: {e}")
        
        if output_path:
            click.echo(f"Resultado salvo em: {output_path}")
    
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {file_path}: {e}")
        logger.error(traceback.format_exc())
        click.echo(f"Erro ao processar arquivo {file_path}: {e}")


@cli.command()
@click.option('--input-dir', '-i', required=True, help='Diretório com arquivos a serem processados')
@click.option('--output-dir', '-o', required=True, help='Diretório de saída')
@click.option('--pattern', '-p', default='*.*', help='Padrão de arquivos a processar (glob)')
@click.option('--custodiante', '-c', help='Filtrar por custodiante (BTG, Daycoval, Master, Singulare)')
@click.option('--recursive', '-r', is_flag=True, help='Pesquisar em subdiretórios')
@click.option('--format', type=click.Choice(['csv', 'excel', 'parquet']), default='csv', help='Formato de saída')
@click.option('--log-dir', help='Diretório para logs')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), default='INFO', help='Nível de log')
@click.option('--to-mysql', is_flag=True, help='Salvar dados no banco MySQL (requer arquivo .env)')
@click.option('--mysql-table', default='despesas_fundos', help='Nome da tabela MySQL para salvar os dados')
@click.option('--truncate-table', is_flag=True, help='Limpar tabela MySQL antes de inserir novos dados')
@click.option('--env-path', default=None, help='Caminho para o arquivo .env com credenciais MySQL')
@click.option('--run-procedures', is_flag=True, default=True, help='Executar procedures de padronização após inserção no MySQL')

def process_directory(
    input_dir: str, 
    output_dir: str, 
    pattern: str,
    custodiante: Optional[str],
    recursive: bool,
    format: str,
    log_dir: Optional[str],
    log_level: str,
    to_mysql: bool,
    mysql_table: str,
    truncate_table: bool,
    env_path: Optional[str],
    run_procedures: bool
):
    """Processa todos os arquivos de despesas em um diretório."""
    # Configurar logger
    logger_instance = setup_logger(log_dir or os.path.join(output_dir, "logs"), log_level)
    
    # Listar arquivos
    files = list_files(input_dir, pattern, recursive)
    
    if not files:
        click.echo(f"Nenhum arquivo encontrado em {input_dir} com o padrão '{pattern}'")
        return
    
    logger.info(f"Encontrados {len(files)} arquivos para processamento")
    
    # Inicializar componentes
    config_loader = ConfigLoader()
    transformer = ExpenseTransformer(config_loader)
    normalizer = DataFrameNormalizer()
    loader = DataLoader(output_dir)
    
    # Dataframe para acumular todos os resultados
    all_dfs = []
    
    # Extrair dados de cada arquivo
    for file_path in files:
        try:
            # Detectar ou filtrar por custodiante
            file_custodiante = detect_custodiante(file_path)
            
            if custodiante and file_custodiante != custodiante:
                continue
            
            # Pular arquivos sem custodiante detectado
            if not file_custodiante:
                logger.warning(f"Custodiante não detectado para o arquivo: {file_path}")
                continue
            
            # Selecionar o extrator apropriado
            extractor = None
            if file_custodiante == "BTG":
                extractor = BTGExtractor(config_loader)
            elif file_custodiante == "Daycoval":
                extractor = DaycovalExtractor(config_loader)
            elif file_custodiante == "Master":
                extractor = MasterExtractor(config_loader)
            elif file_custodiante == "Singulare":
                extractor = SingulareExtractor(config_loader)
            else:
                logger.warning(f"Custodiante não suportado: {file_custodiante}")
                continue
            
            # Extração
            logger.info(f"Processando arquivo {file_path} do custodiante {file_custodiante}")
            df = extractor.extract(file_path)
            
            if df.empty:
                logger.warning(f"Nenhum dado extraído do arquivo: {file_path}")
                continue
            
            # Transformação
            df = transformer.transform(df)
            
            # Normalização
            df = normalizer.normalize_columns(df)
            df = normalizer.remove_duplicates(df)
            df = normalizer.fill_missing_values(df)
            
            # Adicionar informações do custodiante
            df['custodiante'] = file_custodiante
            
            # Adicionar à lista de resultados
            all_dfs.append(df)
            
            logger.info(f"Arquivo {file_path} processado com sucesso: {len(df)} registros")
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}: {e}")
            logger.error(traceback.format_exc())
            click.echo(f"Erro ao processar arquivo {file_path}: {e}")
    
    # Verificar se algum arquivo foi processado
    if not all_dfs:
        logger.warning("Nenhum arquivo foi processado com sucesso")
        click.echo("Nenhum arquivo foi processado com sucesso.")
        return
    
    # Concatenar todos os resultados
    all_df = pd.concat(all_dfs, ignore_index=True)
    
    # Salvar o resultado consolidado
    output_path = None
    if format == 'csv':
        output_path = loader.save_to_csv(all_df, filename="despesas_fundos")
    elif format == 'excel':
        output_path = loader.save_to_excel(all_df, filename="despesas_fundos")
    elif format == 'parquet':
        output_path = loader.save_to_parquet(all_df, filename="despesas_fundos")
    
    # Salvar no MySQL, se solicitado
    if to_mysql and not all_df.empty:
        try:
            inserted = loader.save_to_mysql(
                all_df,
                table_name=mysql_table,
                truncate=truncate_table,
                env_path=env_path,
                run_procedures=run_procedures
            )
            if inserted > 0:
                click.echo(f"Dados inseridos no MySQL com sucesso: {inserted} registros.")
            else:
                click.echo("Falha ao inserir dados no MySQL.")
        except Exception as e:
            click.echo(f"Erro ao salvar no MySQL: {e}")
    
    if output_path:
        click.echo(f"Processamento concluído com sucesso. Resultado salvo em: {output_path}")


@cli.command()
@click.argument('file_path')
@click.option('--output-file', '-o', help='Arquivo de saída para o resultado da validação')
def validate(file_path: str, output_file: Optional[str]):
    """Valida um arquivo de custodiante sem processá-lo completamente."""
    if not os.path.exists(file_path):
        click.echo(f"Arquivo não encontrado: {file_path}")
        return
    
    # Detectar custodiante
    custodiante = detect_custodiante(file_path)
    if custodiante is None:
        click.echo(f"Não foi possível detectar o custodiante para o arquivo: {file_path}")
        return
    
    click.echo(f"Custodiante detectado: {custodiante}")
    
    # Inicializar componentes
    config_loader = ConfigLoader()
    
    # Selecionar o extrator apropriado
    extractor = None
    if custodiante == "BTG":
        extractor = BTGExtractor(config_loader)
    elif custodiante == "Daycoval":
        extractor = DaycovalExtractor(config_loader)
    elif custodiante == "Master":
        extractor = MasterExtractor(config_loader)
    elif custodiante == "Singulare":
        extractor = SingulareExtractor(config_loader)
    else:
        click.echo(f"Custodiante não suportado: {custodiante}")
        return
    
    # Validar o arquivo
    valid = extractor.validate_file(file_path)
    
    if valid:
        click.echo(f"Arquivo {file_path} é válido para processamento pelo extrator {custodiante}")
    else:
        click.echo(f"Arquivo {file_path} NÃO é válido para processamento pelo extrator {custodiante}")
    
    # Salvar resultado em um arquivo, se solicitado
    if output_file:
        with open(output_file, 'w') as f:
            f.write(f"Arquivo: {file_path}\n")
            f.write(f"Custodiante: {custodiante}\n")
            f.write(f"Válido: {'Sim' if valid else 'Não'}\n")
            f.write(f"Data da validação: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        click.echo(f"Resultado da validação salvo em: {output_file}")


@cli.command()
def list_custodiantes():
    """Lista os custodiantes suportados e suas configurações."""
    config_loader = ConfigLoader()
    map_files = config_loader.get_config("map_files")
    
    custodiantes = map_files.get("custodiantes", {})
    
    click.echo("Custodiantes suportados:")
    for custodiante, config in custodiantes.items():
        click.echo(f"\n{custodiante}:")
        click.echo(f"  Tipo de arquivo: {config.get('file_type', 'Qualquer')}")
        click.echo(f"  Pular linhas: {config.get('skip_rows', 0)}")
        click.echo(f"  Encoding: {config.get('encoding', 'utf-8')}")
        click.echo(f"  Separador: {config.get('separator', ';')}")
        
        # Mostrar colunas configuradas
        click.echo("  Colunas configuradas:")
        for col_name, col_config in config.get("columns", {}).items():
            click.echo(f"    {col_name} -> {col_config.get('name')} ({col_config.get('type')})")


if __name__ == "__main__":
    cli()