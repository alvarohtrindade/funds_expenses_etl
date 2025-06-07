"""
Módulo para carregar dados em banco de dados MySQL.
"""
import os
import pandas as pd
from typing import Dict, Optional, Union
from loguru import logger
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

class MySQLLoader:
    """Classe responsável por carregar dados em banco MySQL."""

    def __init__(self, env_path: str = None):
        """
        Inicializa o carregador MySQL.

        Args:
            env_path: Caminho para o arquivo .env com as credenciais.
        """
        # Carregar variáveis de ambiente
        if env_path and os.path.exists(env_path):
            load_dotenv(env_path)
        else:
            load_dotenv()  # Tenta carregar do diretório atual
        
        # Obter credenciais do ambiente
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'DW_DESENV'),
            'port': int(os.getenv('DB_PORT', '3306'))
        }
        
        self.conn = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados MySQL.

        Returns:
            True se a conexão for bem-sucedida, False caso contrário.
        """
        try:
            # Tenta conectar diretamente com o banco de dados
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info(f"Conexão estabelecida com o banco {self.db_config['database']}")
            return True
        except Error as e:
            # Se o banco não existir, tenta criar
            if "Unknown database" in str(e):
                try:
                    # Configuração sem o banco
                    config_without_db = self.db_config.copy()
                    database_name = config_without_db.pop('database')
                    
                    # Conectar sem o banco
                    temp_conn = mysql.connector.connect(**config_without_db)
                    temp_cursor = temp_conn.cursor()
                    
                    # Criar o banco
                    temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                    temp_conn.commit()
                    
                    # Fechar conexão temporária
                    temp_cursor.close()
                    temp_conn.close()
                    
                    # Tentar conectar novamente com o banco
                    self.conn = mysql.connector.connect(**self.db_config)
                    self.cursor = self.conn.cursor()
                    
                    logger.info(f"Banco {database_name} criado e conexão estabelecida")
                    return True
                except Error as inner_e:
                    logger.error(f"Erro ao criar banco de dados: {inner_e}")
                    return False
            
            logger.error(f"Erro ao conectar ao MySQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """
        Fecha a conexão com o banco de dados.
        """
        if self.cursor:
            self.cursor.close()
        
        if self.conn:
            self.conn.close()
            logger.info("Conexão com o banco encerrada")
    
    def create_table_if_not_exists(self) -> bool:
        """
        Cria a tabela de despesas se não existir.

        Returns:
            True se bem-sucedido, False caso contrário.
        """
        try:
            if not self.conn or not self.cursor:
                if not self.connect():
                    return False
            
            # SQL para criar a tabela
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS despesas_fundos (
                idcontrole INT AUTO_INCREMENT PRIMARY KEY,
                data DATE NOT NULL,
                nmfundo VARCHAR(255) NOT NULL,
                nmcategorizado VARCHAR(255),
                lancamento VARCHAR(255) NOT NULL,
                lancamento_original VARCHAR(255),
                valor DECIMAL(15, 2) NOT NULL,
                tipo_lancamento VARCHAR(50) NOT NULL,
                categoria VARCHAR(50),
                observacao TEXT,
                custodiante VARCHAR(100),
                TpFundo VARCHAR(50),
                ano INT,
                mes VARCHAR(20),
                DataETL TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            logger.info("Tabela despesas_fundos criada ou já existente")
            return True
        except Error as e:
            logger.error(f"Erro ao criar tabela: {e}")
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str = 'despesas_fundos') -> int:
        """
        Insere os dados do DataFrame no banco.

        Args:
            df: DataFrame com os dados a serem inseridos.
            table_name: Nome da tabela de destino.

        Returns:
            Número de registros inseridos.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para inserção no MySQL")
            return 0
        
        try:
            if not self.conn or not self.cursor:
                if not self.connect():
                    return 0
            
            # Replace NaN values with None for SQL compatibility
            df = df.replace({pd.NA: None, float('nan'): None})
            
            # Garantir que as colunas no DataFrame correspondam às do banco
            expected_columns = [
                'data', 'nmfundo', 'nmcategorizado', 'lancamento', 'lancamento_original',
                'valor', 'tipo_lancamento', 'categoria', 'observacao',
                'custodiante', 'TpFundo', 'ano', 'mes'
            ]
            
            # Verificar e ajustar colunas no DataFrame
            for col in expected_columns:
                alt_col = col
                
                # Algumas colunas podem ter nomes alternativos
                if col == 'nmfundo' and col not in df.columns and 'nome_fundo' in df.columns:
                    df['nmfundo'] = df['nome_fundo']
                    alt_col = 'nome_fundo'
                elif col == 'TpFundo' and col not in df.columns and 'tpfundo' in df.columns:
                    df['TpFundo'] = df['tpfundo']
                    alt_col = 'tpfundo'
                
                # Se a coluna não existe, criar vazia
                if col not in df.columns:
                    df[col] = None
            
            # Preparar consulta SQL
            columns_str = ', '.join(expected_columns)
            placeholders = ', '.join(['%s'] * len(expected_columns))
            
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Preparar registros
            records = []
            for _, row in df.iterrows():
                record = []
                for col in expected_columns:
                    val = row.get(col)
                    if pd.isna(val):
                        val = None
                    elif col == 'valor':
                        val = float(val)
                    record.append(val)
                
                records.append(tuple(record))
            
            # Inserir registros
            self.cursor.executemany(sql, records)
            self.conn.commit()
            
            inserted = self.cursor.rowcount
            logger.info(f"{inserted} registros inseridos na tabela {table_name}")
            return inserted
        
        except Error as e:
            logger.error(f"Erro ao inserir dados no MySQL: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
        except Exception as e:
            logger.error(f"Erro inesperado ao inserir dados: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def truncate_table(self, table_name: str = 'despesas_fundos') -> bool:
        """
        Limpa todos os dados da tabela.

        Args:
            table_name: Nome da tabela a ser limpa.

        Returns:
            True se bem-sucedido, False caso contrário.
        """
        try:
            if not self.conn or not self.cursor:
                if not self.connect():
                    return False
            
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()
            logger.info(f"Tabela {table_name} limpa com sucesso")
            return True
        except Error as e:
            logger.error(f"Erro ao limpar tabela {table_name}: {e}")
            return False
        
    def execute_procedure(self, procedure_sql: str) -> bool:
        """
        Executa uma procedure SQL no banco de dados.

        Args:
            procedure_sql: String SQL da chamada da procedure.

        Returns:
            True se executada com sucesso, False caso contrário.
        """
        try:
            if not self.conn or not self.cursor:
                if not self.connect():
                    return False
            
            # Executar a procedure
            self.cursor.execute(procedure_sql)
            self.conn.commit()
            logger.info(f"Procedure executada com sucesso: {procedure_sql}")
            return True
        except Error as e:
            logger.error(f"Erro ao executar procedure {procedure_sql}: {e}")
            if self.conn:
                self.conn.rollback()
            return False