"""
Módulo com as classes de carregamento de dados.
"""
import os
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from loguru import logger
import sqlite3
from datetime import datetime

from src.utils.config_loader import ConfigLoader


class DataLoader:
    """Classe responsável pelo carregamento de dados transformados."""

    def __init__(self, output_dir: str = None, config_loader: ConfigLoader = None):
        """
        Inicializa o carregador de dados.

        Args:
            output_dir: Diretório para saída de arquivos.
            config_loader: Instância do carregador de configurações.
        """
        if output_dir is None:
            self.output_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                "data", "output"
            )
        else:
            self.output_dir = output_dir
        
        self.config_loader = config_loader or ConfigLoader()
        
        # Criar diretório de saída se não existir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def save_to_csv(
        self, 
        df: pd.DataFrame, 
        filename: str = None, 
        include_timestamp: bool = True
    ) -> str:
        """
        Salva DataFrame em arquivo CSV.

        Args:
            df: DataFrame a ser salvo.
            filename: Nome do arquivo (sem extensão).
            include_timestamp: Se deve incluir timestamp no nome do arquivo.

        Returns:
            Caminho do arquivo salvo.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para salvar em CSV")
            return None
        
        # Gerar nome do arquivo
        if filename is None:
            filename = "expenses"
        
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_filename = f"{filename}_{timestamp}.csv"
        else:
            full_filename = f"{filename}.csv"
        
        # Caminho completo do arquivo
        file_path = os.path.join(self.output_dir, full_filename)
        
        try:
            # Salvar o DataFrame
            df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"DataFrame salvo com sucesso em: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame em {file_path}: {e}")
            return None
    
    def save_to_excel(
        self, 
        df: pd.DataFrame, 
        filename: str = None, 
        include_timestamp: bool = True
    ) -> str:
        """
        Salva DataFrame em arquivo Excel.

        Args:
            df: DataFrame a ser salvo.
            filename: Nome do arquivo (sem extensão).
            include_timestamp: Se deve incluir timestamp no nome do arquivo.

        Returns:
            Caminho do arquivo salvo.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para salvar em Excel")
            return None
        
        # Gerar nome do arquivo
        if filename is None:
            filename = "expenses"
        
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_filename = f"{filename}_{timestamp}.xlsx"
        else:
            full_filename = f"{filename}.xlsx"
        
        # Caminho completo do arquivo
        file_path = os.path.join(self.output_dir, full_filename)
        
        try:
            # Salvar o DataFrame
            df.to_excel(file_path, index=False, engine='openpyxl')
            logger.info(f"DataFrame salvo com sucesso em: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame em {file_path}: {e}")
            return None
    
    def save_to_sqlite(
        self, 
        df: pd.DataFrame, 
        db_path: str = None, 
        table_name: str = 'expenses'
    ) -> bool:
        """
        Salva DataFrame em banco de dados SQLite.

        Args:
            df: DataFrame a ser salvo.
            db_path: Caminho do banco de dados MySQL.
            table_name: Nome da tabela.

        Returns:
            True se o salvamento foi bem-sucedido, False caso contrário.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para salvar em MySQL")
            return False
        
        # Gerar caminho do banco de dados
        if db_path is None:
            db_path = os.path.join(self.output_dir, "expenses.db")
        
        try:
            # Conectar ao banco de dados
            conn = sqlite3.connect(db_path)
            
            # Salvar o DataFrame
            df.to_sql(
                table_name, 
                conn, 
                if_exists='append',  # Opções: 'replace', 'append', 'fail'
                index=False
            )
            
            conn.close()
            logger.info(f"DataFrame salvo com sucesso na tabela {table_name} do banco {db_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame em SQLite {db_path}: {e}")
            return False
    
    def save_to_parquet(
        self, 
        df: pd.DataFrame, 
        filename: str = None, 
        include_timestamp: bool = True
    ) -> str:
        """
        Salva DataFrame em arquivo Parquet.

        Args:
            df: DataFrame a ser salvo.
            filename: Nome do arquivo (sem extensão).
            include_timestamp: Se deve incluir timestamp no nome do arquivo.

        Returns:
            Caminho do arquivo salvo.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para salvar em Parquet")
            return None
        
        # Gerar nome do arquivo
        if filename is None:
            filename = "expenses"
        
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_filename = f"{filename}_{timestamp}.parquet"
        else:
            full_filename = f"{filename}.parquet"
        
        # Caminho completo do arquivo
        file_path = os.path.join(self.output_dir, full_filename)
        
        try:
            # Salvar o DataFrame
            df.to_parquet(file_path, index=False)
            logger.info(f"DataFrame salvo com sucesso em: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame em {file_path}: {e}")
            return None
        
    def save_to_mysql(
        self, 
        df: pd.DataFrame, 
        table_name: str = 'despesas_fundos',
        truncate: bool = False,
        env_path: str = None,
        run_procedures: bool = True  # Novo parâmetro para controlar execução das procedures
    ) -> int:
        """
        Salva DataFrame no banco de dados MySQL e executa procedures de padronização.

        Args:
            df: DataFrame com os dados a serem inseridos.
            table_name: Nome da tabela de destino.
            truncate: Se deve limpar a tabela antes de inserir.
            env_path: Caminho para o arquivo .env com credenciais.
            run_procedures: Se deve executar procedures de padronização após inserção.

        Returns:
            Número de registros inseridos.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para inserção no MySQL")
            return 0
        
        try:
            from src.utils.mysql_loader import MySQLLoader
            from src.etl.transformers.transformer import DataFrameNormalizer
            
            # Preparar DataFrame para o banco
            df_for_db = df.copy()
            
            # Renomear colunas para o formato do banco
            normalizer = DataFrameNormalizer()
            df_for_db = normalizer.rename_columns_for_db(df_for_db)
            
            # Criar instância do carregador MySQL
            mysql_loader = MySQLLoader(env_path)
            
            # Conectar ao banco
            if not mysql_loader.connect():
                return 0
            
            # Criar tabela se não existir
            if not mysql_loader.create_table_if_not_exists():
                mysql_loader.disconnect()
                return 0
            
            # Limpar tabela se solicitado
            if truncate:
                mysql_loader.truncate_table(table_name)
            
            # Inserir dados
            inserted = mysql_loader.insert_dataframe(df_for_db, table_name)
            
            # Se a inserção foi bem-sucedida e procedures solicitadas, executá-las
            if inserted > 0 and run_procedures:
                logger.info("Executando procedures de padronização...")
                
                # Executa as procedures de padronização
                try:
                    mysql_loader.execute_procedure("CALL DW_DESENV.padronizar_nmcategorizado_despesas()")
                    logger.info("Procedure padronizar_nmcategorizado_despesas executada com sucesso")
                    
                    mysql_loader.execute_procedure("CALL DW_DESENV.padronizar_nmfundo_despesas()")
                    logger.info("Procedure padronizar_nmfundo_despesas executada com sucesso")
                except Exception as proc_error:
                    logger.error(f"Erro ao executar procedures: {proc_error}")
            
            # Fechar conexão
            mysql_loader.disconnect()
            
            logger.info(f"DataFrame salvo com sucesso no MySQL: {inserted} registros")
            return inserted
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame em MySQL: {e}")
            return 0