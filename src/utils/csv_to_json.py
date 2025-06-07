"""
Utilitário para converter dados de mapeamento FIC-FIDC de CSV para JSON.
"""
import os
import pandas as pd
import json
from typing import Dict, List
from loguru import logger

def convert_fic_fidc_csv_to_json(csv_path: str, json_path: str) -> bool:
    """
    Converte o arquivo CSV de mapeamento FIC-FIDC para JSON.
    
    Args:
        csv_path: Caminho para o arquivo CSV.
        json_path: Caminho para salvar o arquivo JSON.
    
    Returns:
        True se a conversão foi bem-sucedida, False caso contrário.
    """
    try:
        # Tentar diferentes codificações
        encodings = ['latin1', 'cp1252', 'ISO-8859-1', 'utf-8-sig', 'utf-8']
        
        df = None
        for encoding in encodings:
            try:
                # Carregar o CSV com a codificação atual
                df = pd.read_csv(csv_path, encoding=encoding)
                logger.info(f"Arquivo CSV lido com sucesso usando encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            logger.error("Não foi possível ler o arquivo CSV com nenhuma das codificações tentadas")
            return False
        
        # Remover valores nulos e normalizar nomes
        df = df.fillna('')
        
        # Normalizar nomes (remover espaços extras, converter para maiúsculas)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip().str.upper()
        
        # Criar dicionários de mapeamento
        fidc_to_fics = {}
        fic_to_fidc = {}
        
        for _, row in df.iterrows():
            fidc = row.get('FIDC', '')
            if fidc and fidc.strip():
                fidc = fidc.strip().upper()
                fics = []
                
                # Adicionar FICs do FIDC
                for fic_col in ['FIC 1', 'FIC 2', 'FIC 3']:
                    if fic_col in df.columns and row.get(fic_col, '') and row.get(fic_col).strip():
                        fic = row.get(fic_col).strip().upper()
                        fics.append(fic)
                        fic_to_fidc[fic] = fidc
                
                if fics:  # Só adicionar FIDCs que tenham FICs associados
                    fidc_to_fics[fidc] = fics
        
        # Criar estrutura de dados JSON
        mapping_data = {
            "fidc_to_fics": fidc_to_fics,
            "fic_to_fidc": fic_to_fidc
        }
        
        # Salvar como JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Mapeamento convertido com sucesso: {json_path}")
        logger.info(f"Total: {len(fidc_to_fics)} FIDCs e {len(fic_to_fidc)} FICs")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao converter mapeamento: {e}")
        return False

if __name__ == "__main__":
    # Configura o logger
    logger.remove()
    logger.add(lambda msg: print(msg))
    
    # Exemplo de uso
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    csv_path = os.path.join(base_dir, "data", "input", "fic_fidc_mapping.csv")
    json_path = os.path.join(base_dir, "config", "fic_fidc_mapping.json")
    
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    
    if convert_fic_fidc_csv_to_json(csv_path, json_path):
        print(f"Conversão concluída: {json_path}")
    else:
        print("Erro na conversão")