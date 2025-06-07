#!/usr/bin/env python3
"""
Script de debug para testar o mapeamento FIC-FIDC de forma isolada.
"""
import pandas as pd
import sys
import os

# Adicionar o diretório src ao path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, 'src'))

from etl.transformers.transformer import ExpenseTransformer
from utils.config_loader import ConfigLoader

def debug_fic_fidc_mapping():
    """
    Testa o mapeamento FIC-FIDC com dados específicos do problema.
    """
    print("🔍 SCRIPT DE DEBUG - Teste FIC-FIDC")
    print("=" * 50)
    
    # Criar dados de teste baseados nos dados reais reportados
    test_data = pd.DataFrame([
        # Dados que deveriam ser AROEIRA FIC FIM (IPÊ)
        {
            'nome_fundo': 'AROEIRA FIC FIM CP',
            'data': '2025-05-07',
            'lancamento': '000525906 - REF A RESGATE DE COTAS NO FUNDO TESOURO SELIC FI RF',
            'valor': 12649.14,
            'tipo_lancamento': 'Crédito',
            'observacao': None,
            'remetente': None,
            'custodiante': 'BTG'
        },
        # Dados que deveriam ser SECULO FIC FIM (IPÊ)
        {
            'nome_fundo': 'SECULO FIC FIM CP',
            'data': '2025-05-07', 
            'lancamento': '000525904 - REF A RESGATE DE COTAS NO FUNDO TESOURO SELIC FI RF',
            'valor': 807.45,
            'tipo_lancamento': 'Crédito',
            'observacao': None,
            'remetente': None,
            'custodiante': 'BTG'
        },
        # Dados que deveriam ser ATICO (BRAVOS)
        {
            'nome_fundo': 'ATICO FC FIM CP',
            'data': '2025-05-07',
            'lancamento': '004861940 - REF A RESGATE DE COTAS NO FUNDO TESOURO SELIC FI RF',
            'valor': 20238.66,
            'tipo_lancamento': 'Crédito',
            'observacao': None,
            'remetente': None,
            'custodiante': 'BTG'
        },
        # Dados que deveriam permanecer como FIDC
        {
            'nome_fundo': 'LION FC FIDC',
            'data': '2025-05-07',
            'lancamento': 'Taxa de Administração - Operação direta FIDC',
            'valor': 5000.00,
            'tipo_lancamento': 'Débito',
            'observacao': 'Taxa administrativa do FIDC',
            'remetente': 'Administradora',
            'custodiante': 'BTG'
        }
    ])
    
    print(f"📊 Dados de teste criados: {len(test_data)} linhas")
    print("\nDados ANTES da transformação:")
    for i, row in test_data.iterrows():
        print(f"  {i+1}. {row['nome_fundo']} - {row['lancamento'][:30]}...")
    
    # Criar transformer e aplicar transformações
    try:
        config_loader = ConfigLoader()
        transformer = ExpenseTransformer(config_loader)
        
        print(f"\n🔧 Aplicando transformações...")
        
        # Simular o processamento passo a passo
        df = test_data.copy()
        
        # Adicionar colunas necessárias
        df = transformer._add_required_columns(df)
        
        # Determinar tipos de fundos
        df['TpFundo'] = df['nome_fundo'].apply(transformer._determine_fund_type)
        print(f"\n📈 Tipos determinados: {df['TpFundo'].value_counts().to_dict()}")
        
        # Aplicar mapeamento FIC-FIDC
        df_resultado = transformer._categorize_funds(df)
        
        print("\n📈 Resultados APÓS transformação:")
        print("-" * 80)
        print(f"{'#':<3} {'NMFUNDO':<15} {'TPFUNDO':<8} {'NMCATEGORIZADO':<20} {'LANCAMENTO':<30}")
        print("-" * 80)
        
        for i, row in df_resultado.iterrows():
            lancamento_short = str(row['lancamento'])[:30] + "..." if len(str(row['lancamento'])) > 30 else str(row['lancamento'])
            print(f"{i+1:<3} {str(row['nmfundo']):<15} {str(row['TpFundo']):<8} {str(row['nmcategorizado']):<20} {lancamento_short:<30}")
        
        print("\n✅ VALIDAÇÕES:")
        
        # Validação 1: Verificar se AROEIRA foi mapeada para IPÊ
        aroeira_rows = df_resultado[df_resultado['nmcategorizado'] == 'AROEIRA FIC FIM']
        if not aroeira_rows.empty:
            aroeira_nmfundo = aroeira_rows.iloc[0]['nmfundo']
            aroeira_tpfundo = aroeira_rows.iloc[0]['TpFundo']
            print(f"   ✓ AROEIRA: nmfundo='{aroeira_nmfundo}' (esperado: IPÊ), TpFundo='{aroeira_tpfundo}' (esperado: FICFIM)")
            if aroeira_nmfundo == 'IPÊ' and aroeira_tpfundo in ['FICFIM', 'FIC FIM']:
                print("     ✅ AROEIRA mapeada CORRETAMENTE!")
            else:
                print("     ❌ AROEIRA NÃO mapeada corretamente!")
        else:
            print("   ❌ AROEIRA não encontrada nos resultados")
        
        # Validação 2: Verificar se SECULO foi mapeada para IPÊ
        seculo_rows = df_resultado[df_resultado['nmcategorizado'] == 'SECULO FIC FIM']
        if not seculo_rows.empty:
            seculo_nmfundo = seculo_rows.iloc[0]['nmfundo']
            seculo_tpfundo = seculo_rows.iloc[0]['TpFundo']
            print(f"   ✓ SECULO: nmfundo='{seculo_nmfundo}' (esperado: IPÊ), TpFundo='{seculo_tpfundo}' (esperado: FICFIM)")
            if seculo_nmfundo == 'IPÊ' and seculo_tpfundo in ['FICFIM', 'FIC FIM']:
                print("     ✅ SECULO mapeada CORRETAMENTE!")
            else:
                print("     ❌ SECULO NÃO mapeada corretamente!")
        else:
            print("   ❌ SECULO não encontrada nos resultados")
        
        # Validação 3: Verificar se ATICO foi mapeada para BRAVOS
        atico_rows = df_resultado[df_resultado['nmcategorizado'] == 'ATICO']
        if not atico_rows.empty:
            atico_nmfundo = atico_rows.iloc[0]['nmfundo']
            atico_tpfundo = atico_rows.iloc[0]['TpFundo']
            print(f"   ✓ ATICO: nmfundo='{atico_nmfundo}' (esperado: BRAVOS), TpFundo='{atico_tpfundo}' (esperado: FICFIM)")
            if atico_nmfundo == 'BRAVOS' and atico_tpfundo in ['FICFIM', 'FIC FIM']:
                print("     ✅ ATICO mapeada CORRETAMENTE!")
            else:
                print("     ❌ ATICO NÃO mapeada corretamente!")
        else:
            print("   ❌ ATICO não encontrada nos resultados")
        
        # Validação 4: Verificar se FIDC foi mantido
        fidc_rows = df_resultado[df_resultado['TpFundo'] == 'FIDC']
        if not fidc_rows.empty:
            print(f"   ✓ FIDCs mantidos: {len(fidc_rows)} linha(s)")
            for _, row in fidc_rows.iterrows():
                print(f"     - {row['nmfundo']} (nmcategorizado: {row['nmcategorizado']})")
        else:
            print("   ❌ Nenhum FIDC encontrado - pode estar incorreto")
        
        print("\n📋 RESUMO FINAL:")
        summary = df_resultado.groupby(['TpFundo', 'nmfundo']).size().reset_index(name='count')
        for _, row in summary.iterrows():
            print(f"   - {row['TpFundo']} | {row['nmfundo']}: {row['count']} linha(s)")
        
        return df_resultado
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    resultado = debug_fic_fidc_mapping()
    
    if resultado is not None:
        print("\n🎉 TESTE CONCLUÍDO!")
        print("\nPara executar com dados reais:")
        print("1. Substitua os arquivos corrigidos")
        print("2. Execute: fund-etl process-directory --input-dir ./data/input --output-dir ./data/output --to-mysql --truncate-table")
        print("3. Verifique os resultados no banco de dados")
    else:
        print("\n💥 TESTE FALHOU - Verifique os erros acima")