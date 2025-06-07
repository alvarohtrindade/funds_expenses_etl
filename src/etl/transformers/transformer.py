"""
Módulo com as classes de transformação de dados.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from loguru import logger
import re
from datetime import datetime

from src.utils.config_loader import ConfigLoader


class ExpenseTransformer:
    """Classe responsável por transformar e padronizar dados de despesas."""

    def __init__(self, config_loader: ConfigLoader = None):
        """
        Inicializa o transformador.

        Args:
            config_loader: Instância do carregador de configurações.
        """
        self.config_loader = config_loader or ConfigLoader()
        self.lancamentos_map = self.config_loader.get_config("map_lancamentos")
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma e padroniza um DataFrame de despesas.

        Args:
            df: DataFrame a ser transformado.

        Returns:
            DataFrame transformado.
        """
        if df.empty:
            logger.warning("DataFrame vazio fornecido para transformação")
            return df
        
        # Cria uma cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Preservar o lançamento original antes da normalização
        if 'lancamento' in df.columns:
            df['lancamento_original'] = df['lancamento'].copy()
        
        # Normaliza os lançamentos
        df = self._normalize_lancamentos(df)
        
        # Adiciona colunas necessárias
        df = self._add_required_columns(df)
        
        # Formata as datas
        df = self._format_dates(df)
        
        # Categoriza as despesas
        df = self._categorize_expenses(df)
        
        # Determinar tipo de fundo
        df['TpFundo'] = df['nome_fundo'].apply(self._determine_fund_type)
        logger.info(f"Tipos identificados automaticamente: {df['TpFundo'].value_counts().to_dict()}")
        
        # Categorizar os fundos com base no mapeamento FIC-FIDC
        df = self._categorize_funds(df)
        
        # Aplicar mapeamento manual para corrigir tipos "Outro"
        df = self._apply_manual_fund_type_mapping(df)
        logger.info(f"Tipos após mapeamento manual: {df['TpFundo'].value_counts().to_dict()}")
        
        # Padronizar tipos de fundos
        df = self._standardize_fund_types(df)
        
        # Classificar lançamentos como despesas
        df = self._classify_despesas(df)
        
        return df
    
    def _standardize_fund_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Padroniza os tipos de fundos na coluna TpFundo.
        """
        if 'TpFundo' not in df.columns:
            return df
        
        # Mapeamento para padronização
        type_mapping = {
            # FIC FIM e variações
            'FIC FIM': 'FICFIM',
            
            # FIC e variações
            'FIC II': 'FIC',
            
            # FIM CP e variações
            'FIM CP': 'FIM CP',
            'FICFIM CP': 'FICFIM CP',
            
            # Tipos padrão - manter como estão
            'FIDC': 'FIDC',
            'FICFIDC': 'FICFIDC',
            'FIP': 'FIP',
            'FIM': 'FIM',
            'FIA': 'FIA',
            'FICFIA': 'FICFIA',
            'FICFIM': 'FICFIM',
            'FIC': 'FIC'
        }
        
        # Distribuição inicial
        tipos_iniciais = df['TpFundo'].value_counts().to_dict()
        logger.info(f"Distribuição de tipos antes da padronização: {tipos_iniciais}")
        
        # Aplicar padronização - preservar valores que já estão padronizados
        df['TpFundo'] = df['TpFundo'].apply(lambda x: type_mapping.get(x, x))
        
        # Distribuição final
        tipos_finais = df['TpFundo'].value_counts().to_dict()
        logger.info(f"Distribuição de tipos após padronização: {tipos_finais}")
        
        return df
    
    def _determine_fund_type(self, fund_name: str) -> str:
        """
        Determina o tipo do fundo com base no nome de forma mais robusta.
        Reconhece abreviações, variações de formatação e sufixos comuns.
        """
        if not fund_name or not isinstance(fund_name, str):
            return "Outro"
        
        # Normalizar nome do fundo
        fund_name = str(fund_name).upper().strip()
        
        # Lista de palavras-chave e sufixos a ignorar na análise
        suffixes_to_ignore = [
            "RL", "- RL", "RESP LIMITADA", "RESPONSABILIDAD LIMITADA", 
            "RESPONSABILIDADE LIMTADA", "RESPONSABILIDADE LIMITADA",
            "NP", "- NP", "SUBORDINADA", "- SUBORDINADA"
        ]
        
        # Remover sufixos que não afetam a classificação
        clean_name = fund_name
        for suffix in suffixes_to_ignore:
            if suffix in clean_name:
                clean_name = clean_name.replace(suffix, "").strip()
        
        # Padrões para identificação
        patterns = {
            "FIDC": [
                "FIDC", 
                "FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS",
                "FUNDO DE INVESTIMENTO EM DIREITOS",
                "FUNDO DE INVEST. EM DIREITOS",
                "FUNDO DE INVESTIMENTO EM DC",
                "EM DIREITOS C"
            ],
            "FICFIDC": [
                "FICFIDC", 
                "FIC FIDC",
                "FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM DIREITOS"
            ],
            "FIC FIM": [
                "FIC FIM", 
                "FC FIM",
                "FIC DE FIM",
                "FUNDO DE INVESTIMENTO EM COTAS DE FUNDO MULTIMERCADO"
            ],
            "FICFIM": [
                "FICFIM", 
                "FIC FIM"
            ],
            "FICFIM CP": [
                "FICFIM CP", 
                "FIC FIM CP", 
                "FIC DE FIM CP"
            ],
            "FIM": [
                "FIM", 
                "FUNDO DE INVESTIMENTO MULTIMERCADO", 
                "MULTIMERCADO"
            ],
            "FIC": [
                "FIC", 
                "FC", 
                "FUNDO DE INVESTIMENTO EM COTAS"
            ],
            "FIM CP": [
                "FIM CP"
            ],
            "FIA": [
                "FIA", 
                "FUNDO DE INVESTIMENTO EM AÇÕES"
            ],
            "FICFIA": [
                "FICFIA", 
                "FIC FIA", 
                "FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE AÇÕES"
            ]
        }
        
        # Verificar o tipo com base nos padrões
        for fund_type, type_patterns in patterns.items():
            for pattern in type_patterns:
                if pattern in clean_name:
                    return fund_type
        
        # Casos especiais para abreviações DC (Direitos Creditórios)
        if " EM DC" in clean_name or "EM DIREITOS" in clean_name:
            return "FIDC"
        
        # Se chegou aqui, verificar se o próprio nome contém uma classificação
        if "FIDC" in clean_name:
            return "FIDC"
        elif "FIC FIM CP" in clean_name:
            return "FICFIM CP"
        elif "FIC FIM" in clean_name:
            return "FICFIM"
        elif "FIC" in clean_name and "FIM" in clean_name:
            return "FICFIM"
        elif "FIM CP" in clean_name:
            return "FIM CP"
        elif "FIM" in clean_name:
            return "FIM"
        elif "FIC" in clean_name:
            return "FIC"
        
        # Se não identificou nenhum padrão
        return "Outro"

    def _categorize_funds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Categoriza os fundos com base no mapeamento FIDC-FIC.
        Distribui as linhas entre o FIDC original e seus FICs sem duplicação.
        """
        if 'nome_fundo' not in df.columns:
            logger.warning("Coluna 'nome_fundo' não encontrada no DataFrame")
            return df
        
        # Cria cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Carregamos o mapeamento FIC-FIDC
        fidc_to_fics, fic_to_fidc = self.config_loader.load_fic_fidc_mapping()
        
        # LOGS INICIAIS
        logger.info(f"Processando {len(df)} linhas, {df['nome_fundo'].nunique()} fundos únicos")
        logger.info(f"Fundos encontrados: {', '.join(df['nome_fundo'].unique()[:5])}" + 
                (f" e mais {df['nome_fundo'].nunique() - 5} outros..." if df['nome_fundo'].nunique() > 5 else ""))
        
        # Normalizar nomes para lidar com acentos/variações
        import unicodedata
        
        def normalize_string(s):
            if not isinstance(s, str):
                return str(s)
            s = s.strip().upper()
            # Remover acentos
            s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
            return s
        
        # Criar versões normalizadas dos mapeamentos para buscas
        normalized_fidc_to_fics = {}
        for fidc, fics in fidc_to_fics.items():
            normalized_fidc_to_fics[normalize_string(fidc)] = fics
        
        # Inicialmente, nmcategorizado = nome_fundo (para casos sem mapeamento)
        df['nmcategorizado'] = df['nome_fundo']
        
        # Processar cada fundo que é um FIDC
        for fidc_name, fics_list in fidc_to_fics.items():
            if not fics_list or fics_list[0] == "-":  # Ignora FICs marcados como "-"
                continue
                
            # Encontrar todas as linhas deste FIDC
            fidc_mask = df['nome_fundo'].apply(lambda x: normalize_string(fidc_name) in normalize_string(x) or 
                                                normalize_string(x) in normalize_string(fidc_name))
            
            fidc_rows = df[fidc_mask]
            if len(fidc_rows) == 0:
                continue
                
            logger.debug(f"Encontradas {len(fidc_rows)} linhas para o FIDC: {fidc_name}")
            
            # Número de linhas a distribuir para cada FIC
            total_rows = len(fidc_rows)
            num_fics = len(fics_list)
            
            # Garantir que pelo menos 50% das linhas permaneçam com o FIDC original
            keep_original_count = max(int(total_rows * 0.5), 1)
            rows_to_distribute = total_rows - keep_original_count
            
            # Se não houver linhas suficientes para distribuir, continuar
            if rows_to_distribute <= 0:
                continue
                
            # Distribuir as linhas restantes entre os FICs
            rows_per_fic = max(1, rows_to_distribute // num_fics)
            
            # Obter todos os índices das linhas do FIDC
            fidc_indices = fidc_rows.index.tolist()
            
            # Manter alguns índices com o FIDC original e distribuir os demais
            import random
            random.seed(42)  # Para consistência
            
            # Shuffle índices para distribuição aleatória mas consistente
            random.shuffle(fidc_indices)
            
            # Índices a manter com o FIDC original
            keep_indices = fidc_indices[:keep_original_count]
            
            # Índices a distribuir entre os FICs
            distribute_indices = fidc_indices[keep_original_count:]
            
            # Distribuir entre os FICs
            for i, fic in enumerate(fics_list):
                start_idx = i * rows_per_fic
                end_idx = min((i + 1) * rows_per_fic, len(distribute_indices))
                
                if start_idx >= len(distribute_indices):
                    break
                    
                fic_indices = distribute_indices[start_idx:end_idx]
                
                if not fic_indices:
                    continue
                    
                # Atribuir o FIC como nmcategorizado para estas linhas
                df.loc[fic_indices, 'nmcategorizado'] = fic
                
                # Determina o tipo correto para o FIC
                fic_type = self._determine_fic_type(fic)
                df.loc[fic_indices, 'TpFundo'] = fic_type
                
                logger.debug(f"Atribuído {len(fic_indices)} linhas ao FIC {fic} com tipo {fic_type}")
        
        # LOGS FINAIS
        categorized_count = (df['nome_fundo'] != df['nmcategorizado']).sum()
        logger.info(f"Fundos categorizados: {categorized_count} mapeamentos aplicados")
        
        if categorized_count > 0:
            # Mostrar estatísticas de categorização
            logger.info(f"Distribuição de nmcategorizado: {df['nmcategorizado'].value_counts().head(10).to_dict()}")
            logger.info(f"Distribuição de TpFundo: {df['TpFundo'].value_counts().to_dict()}")
            
            # Mostrar exemplos de mapeamentos realizados
            sample_map = df[df['nome_fundo'] != df['nmcategorizado']].head(5)
            for _, row in sample_map.iterrows():
                logger.debug(f"Exemplo de mapeamento: {row['nome_fundo']} -> {row['nmcategorizado']} (TpFundo: {row['TpFundo']})")
        
        return df

    def _determine_fic_type(self, fic_name: str) -> str:
        """
        Determina o tipo do FIC com base no nome.
        """
        if not fic_name or not isinstance(fic_name, str):
            return "Outro"
        
        # Normalizar nome do fundo
        fic_name = str(fic_name).upper().strip()
        
        # Verificar tipo baseado em palavras-chave no nome
        if "FIC FIM CP" in fic_name:
            return "FICFIM CP"
        elif "FIC FIM" in fic_name:
            return "FICFIM"
        elif "FIC FIA" in fic_name:
            return "FICFIA"
        elif "FIC" in fic_name:
            return "FIC"
        elif "FIM CP" in fic_name:
            return "FIM CP"
        elif "FIM" in fic_name:
            return "FIM"
        elif "FIA" in fic_name:
            return "FIA"
        
        # Caso não identifique um padrão específico
        return "Outro"

    def _determine_fic_type(self, fic_name: str) -> str:
        """
        Determina o tipo do FIC com base no nome.
        """
        if not fic_name or not isinstance(fic_name, str):
            return "Outro"
        
        # Normalizar nome do fundo
        fic_name = str(fic_name).upper().strip()
        
        # Verificar tipo baseado em palavras-chave no nome
        if "FIC FIM CP" in fic_name:
            return "FICFIM CP"
        elif "FIC FIM" in fic_name:
            return "FICFIM"
        elif "FIC FIA" in fic_name:
            return "FICFIA"
        elif "FIC" in fic_name:
            return "FIC"
        elif "FIM CP" in fic_name:
            return "FIM CP"
        elif "FIM" in fic_name:
            return "FIM"
        elif "FIA" in fic_name:
            return "FIA"
        
        # Caso não identifique um padrão específico
        return "Outro"

    def _normalize_lancamentos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza os lançamentos conforme o mapeamento.

        Args:
            df: DataFrame com lançamentos originais.

        Returns:
            DataFrame com lançamentos normalizados.
        """
        if 'lancamento' not in df.columns:
            logger.warning("Coluna 'lancamento' não encontrada no DataFrame")
            return df
        
        # Função para normalizar cada lançamento
        def normalize_lancamento(lancamento):
            if not isinstance(lancamento, str) or pd.isna(lancamento):
                return lancamento
            
            # Limpar o texto
            lancamento = lancamento.strip()
            
            # Verificar se o lançamento exato está no mapeamento
            if lancamento in self.lancamentos_map:
                return self.lancamentos_map[lancamento]
            
            # Verificar se parte do lançamento está no mapeamento
            for key, value in self.lancamentos_map.items():
                if key.lower() in lancamento.lower():
                    return value
            
            # Se não encontrar, retorna o original
            return lancamento
        
        # Aplicar a normalização
        df['lancamento'] = df['lancamento'].apply(normalize_lancamento)
        
        return df
    
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona colunas necessárias ao DataFrame.

        Args:
            df: DataFrame original.

        Returns:
            DataFrame com colunas adicionadas.
        """
        # Garantir que as colunas essenciais existam
        required_columns = {
            'data': None,
            'nome_fundo': None,
            'lancamento': None,
            'lancamento_original': None,
            'valor': 0.0,
            'tipo_lancamento': None,
            'custodiante': None,
            'ano': None,
            'mes': None,
            'categoria': None,
            'TpFundo': None,
            'nmcategorizado': None  # Adicionando a coluna nmcategorizado
        }
        
        for col, default_value in required_columns.items():
            if col not in df.columns:
                df[col] = default_value
        
        return df
    
    def _format_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formata e extrai informações de datas.

        Args:
            df: DataFrame com datas.

        Returns:
            DataFrame com datas formatadas e extraídas.
        """
        if 'data' not in df.columns:
            logger.warning("Coluna 'data' não encontrada no DataFrame")
            return df
        
        # Converter para datetime se ainda não for
        if not pd.api.types.is_datetime64_any_dtype(df['data']):
            df['data'] = pd.to_datetime(df['data'], errors='coerce')
        
        # Extrair ano e mês
        df['ano'] = df['data'].dt.year
        
        # Mês em formato nome (português)
        month_names = {
            1: 'Janeiro',
            2: 'Fevereiro',
            3: 'Março',
            4: 'Abril',
            5: 'Maio',
            6: 'Junho',
            7: 'Julho',
            8: 'Agosto',
            9: 'Setembro',
            10: 'Outubro',
            11: 'Novembro',
            12: 'Dezembro'
        }
        
        df['mes'] = df['data'].dt.month.map(month_names)
        
        return df
    
    def _categorize_expenses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Categoriza as despesas em grupos.

        Args:
            df: DataFrame com despesas.

        Returns:
            DataFrame com despesas categorizadas.
        """
        # Mapeamento de categorias de despesas
        categoria_map = {
            'Taxa': ['Taxa', 'Tarifa', 'TAXA', 'TARIFA', 'TX.', 'TXCUSULTORIA', 'CUSTO SELIC'],
            'Custo': ['Custo', 'CUSTO', 'CUSTO_EMISNC', 'COST'],
            'Pagamento': ['Pagamento', 'PAGAMENTO', 'Pagto', 'PGTO'],
            'Transferência': ['Transferência', 'TRANSFERÊNCIA', 'DOC/TED', 'TED', 'DOC', 'TRANSF'],
            'Operação': ['Operação', 'OPERAÇÃO', 'Compra', 'Venda', 'COMPRA', 'VENDA', 'Aquisição', 'AQUISIÇÃO', 'PURCHASE'],
            'Resgate': ['Resgate', 'RESGATE', 'REDEMPTION'],
            'Aplicação': ['Aplicação', 'APLICAÇÃO', 'Aplic.'],
            'Auditoria': ['Auditoria', 'AUDITORIA', '#AUDIT', 'AUDIT'],
            'Custódia': ['Custódia', 'CUSTODIA', 'CUSTODY'],
            'Gestão': ['Gestão', 'GESTÃO'],
            'Liquidação': ['Liquidação', 'LIQUIDAÇÃO', 'LIQUIDACAO'],
            'Outros': []  # Categoria padrão
        }
        
        # Função para determinar a categoria
        def get_categoria(lancamento):
            if not isinstance(lancamento, str) or pd.isna(lancamento):
                return 'Outros'
                
            lancamento = str(lancamento).upper()
            
            for categoria, keywords in categoria_map.items():
                for keyword in keywords:
                    if keyword.upper() in lancamento:
                        return categoria
            
            # Regras especiais para casos comuns
            if "LÍQUIDO NO DIA" in lancamento or "LIQUIDO NO DIA" in lancamento:
                return "Balanço Diário"
            
            return 'Outros'
        
        # Aplicar categorização
        df['categoria'] = df['lancamento'].apply(get_categoria)
        
        return df
    
    def _apply_manual_fund_type_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica mapeamento manual para casos específicos de fundos
        que são difíceis de categorizar automaticamente.
        """
        # Mapeamento manual de nome de fundo para tipo
        manual_mapping = {
            # Exemplos baseados na sua lista
            "ALBAREDO FIDC": "FIDC",
            "Z INVEST FIDC": "FIDC",
            "SC FUNDO DE INVESTIMENTO EM DC": "FIDC",
            "PRIME AGRO FIDC": "FIDC",
            "GRUPO PRIME AGRO FIC": "FICFIM",
            "GLOBAL FUTURA FIDC": "FIDC",
            "BASÃ": "FIDC",
            "BLUE ROCKET FIDC": "FIDC",
            "VISHNU FUNDO": "FIDC",
            "AF6 FIDC": "FIDC",
            "BELL FUNDO": "FIDC",
            "VERGINIA FUNDO": "FIDC",
            "BONTEMPO FIDC": "FIDC",
            "PINPAG FIDC": "FIDC",
            "BELLIN FIC": "FICFIM",
            "NINE CAPITAL FIDC": "FIDC",
            "NINE CAPITAL FIC": "FIC",
            "MASTRENN FIDC": "FIDC",
            "MASTRENN FIC": "FICFIM CP",
            "UKF FIDC": "FIDC",
            "UKF FIC": "FIC",
            "NR11 FUNDO": "FIDC",
            "SMT AGRO HOLDING": "FICFIM",
            "SMT AGRO FUNDO": "FIDC",
            "TERTON FUNDO": "FIDC",
            "ZAB LEGACY FIDC": "FIDC",
            "SCI SAO CR FC FIM CP": "FICFIM CP",
            "ARTANIS FUNDO DE INVESTIMENTO MULTIMERCA": "FIM",
            "GOLIATH FUNDO DE INVESTIMENTO MULTIMERCA": "FIM",
            "AGROCETE FUNDO DE INVESTIMENTO EM DIREIT": "FIDC",
            "CREDILOG II - FUNDO DE INVESTIMENTO EM D": "FIDC",
            "CREDILOG - FUNDO DE INVESTIMENTO EM DIRE": "FIDC",
            "PINPAG  FIDC - RL": "FIDC",
            "CAPITALIZA FUNDO DE INVESTIMENTO EM DIRE": "FIDC",
            "FUTURO CAPITAL FUNDO DE INVESTIMENTO EM": "FIDC",
            "VELSO - FUNDO DE INVESTIMENTO EM DIREITO": "FIDC",
            "ANVERES FUNDO DE INVESTIMENTO EM DIREITO": "FIDC"
        }
        
        # Aplicar mapeamento manual para TpFundo = 'Outro'
        outros_mask = df['TpFundo'] == 'Outro'
        
        if outros_mask.any():
            logger.info(f"Encontrados {outros_mask.sum()} fundos com tipo 'Outro' para mapeamento manual")
            
            # Para cada fundo com tipo "Outro", verificar no mapeamento manual
            for idx, row in df[outros_mask].iterrows():
                fund_name = row['nome_fundo'].upper()
                matched = False
                
                # Tentar correspondências parciais no mapeamento manual
                for key, fund_type in manual_mapping.items():
                    if key in fund_name:
                        df.loc[idx, 'TpFundo'] = fund_type
                        matched = True
                        break
                
                if not matched and idx % 100 == 0:  # Log apenas para alguns casos não mapeados
                    logger.debug(f"Fundo não mapeado manualmente: {fund_name}")
            
            # Log dos resultados após mapeamento manual
            remaining_outros = (df['TpFundo'] == 'Outro').sum()
            logger.info(f"Após mapeamento manual: {outros_mask.sum() - remaining_outros} fundos corrigidos, {remaining_outros} fundos ainda como 'Outro'")
        
        return df

    def _classify_despesas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classifica lançamentos como despesas com base na lista de configuração.
        
        Args:
            df: DataFrame com lançamentos.
            
        Returns:
            DataFrame com a coluna 'Despesa' adicionada.
        """
        # Obter lista de lançamentos classificados como despesas
        despesas_list = self.config_loader.get_despesas_classifications()
        
        # Converter para maiúsculas para comparação case-insensitive
        despesas_list_upper = [d.upper() for d in despesas_list]
        
        # Criar a coluna Despesa
        def is_despesa(lancamento):
            if not isinstance(lancamento, str):
                return False
                
            # Verificar correspondência exata (case-insensitive)
            if lancamento.upper() in despesas_list_upper:
                return True
                
            # Verificar correspondência parcial (para casos como "Taxa de XXX")
            for desp in despesas_list_upper:
                if desp in lancamento.upper():
                    return True
                    
            return False
        
        # Aplicar a função de classificação
        df['Despesa'] = df['lancamento'].apply(is_despesa)
        
        # Log de quantos lançamentos foram classificados como despesas
        despesas_count = df['Despesa'].sum()
        logger.info(f"Classificados {despesas_count} de {len(df)} lançamentos como despesas ({despesas_count/len(df)*100:.2f}%)")
        
        return df


class DataFrameNormalizer:
    """Classe para normalizar e padronizar DataFrames."""
    
    @staticmethod
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza nomes de colunas para um padrão consistente.

        Args:
            df: DataFrame a ser normalizado.

        Returns:
            DataFrame com colunas normalizadas.
        """
        # Cria uma cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Converter para minúsculas e remover espaços extras
        df.columns = [str(col).strip().lower() for col in df.columns]
        
        # Substituir espaços por underscores
        df.columns = [col.replace(' ', '_') for col in df.columns]
        
        # Remover caracteres especiais
        df.columns = [re.sub(r'[^a-z0-9_]', '', col) for col in df.columns]
        
        return df
    
    @staticmethod
    def remove_duplicates(df: pd.DataFrame, subset: List[str] = None) -> pd.DataFrame:
        """
        Remove linhas duplicadas do DataFrame.

        Args:
            df: DataFrame original.
            subset: Lista de colunas a considerar para duplicatas.

        Returns:
            DataFrame sem duplicatas.
        """
        if subset is None:
            subset = ['data', 'nome_fundo', 'lancamento', 'valor']
            
        # Filtrar apenas as colunas existentes
        subset = [col for col in subset if col in df.columns]
        
        if not subset:
            return df
        
        # Remover duplicatas
        return df.drop_duplicates(subset=subset)
    
    @staticmethod
    def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Preenche valores ausentes com valores padrão.

        Args:
            df: DataFrame original.

        Returns:
            DataFrame com valores ausentes preenchidos.
        """
        # Cria uma cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Mapeamento de valores padrão para cada tipo de coluna
        default_values = {
            'numeric': 0,
            'string': '',
            'datetime': None,
            'boolean': False
        }
        
        # Aplicar valores padrão por tipo de coluna
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(default_values['numeric'])
            elif pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].fillna(default_values['string'])
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                # Não preencher datas ausentes
                pass
            elif pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].fillna(default_values['boolean'])
        
        return df
    
    @staticmethod
    def rename_columns_for_db(df: pd.DataFrame) -> pd.DataFrame:
        """
        Renomeia colunas para o padrão do banco de dados.
        
        Args:
            df: DataFrame original.
            
        Returns:
            DataFrame com colunas renomeadas.
        """
        # Cria uma cópia para evitar SettingWithCopyWarning
        df = df.copy()
        
        # Mapeamento de colunas
        column_map = {
            'id': 'idcontrole',
            'nome_fundo': 'nmfundo',
            'tpfundo': 'TpFundo',  # Corrigir para maiúscula
            'created_at': 'DataETL'
        }
        
        # Renomear colunas
        df = df.rename(columns=column_map)
        
        return df