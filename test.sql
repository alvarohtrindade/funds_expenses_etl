UPDATE DW_DESENV.despesas_fundos
SET categoria = CASE
    WHEN LOWER(categoria) LIKE '%cvm%' THEN 'CVM'
    WHEN LOWER(categoria) LIKE '%crdc%' THEN 'CRDC'
    WHEN LOWER(categoria) LIKE '%custod%' THEN 'Taxa de Custódia'
    WHEN LOWER(categoria) LIKE '%admin%' THEN 'Taxa de Administração'
    WHEN LOWER(categoria) LIKE '%gest%' THEN 'Taxa de Gestão'
    WHEN LOWER(categoria) LIKE '%emiss%' AND LOWER(categoria) LIKE '%nota%' THEN 'Custo Emissão NC'
    WHEN LOWER(categoria) LIKE '%anbima%' THEN 'Anbima'
    WHEN LOWER(categoria) LIKE '%consult%' THEN 'Taxa de Consultoria'
    WHEN LOWER(categoria) LIKE '%banc%' AND LOWER(categoria) LIKE '%tarif%' THEN 'Taxa Bancária'
    WHEN LOWER(categoria) LIKE '%advog%' THEN 'Despesas Jurídicas'
    WHEN LOWER(categoria) LIKE '%jurid%' THEN 'Despesas Jurídicas'
    WHEN LOWER(categoria) LIKE '%judic%' THEN 'Despesas Jurídicas'
    WHEN LOWER(categoria) LIKE '%process%' THEN 'Despesas Jurídicas'
    WHEN LOWER(categoria) LIKE '%registro%' AND LOWER(categoria) LIKE '%anbima%' THEN 'Anbima'
    WHEN LOWER(categoria) LIKE '%acesstage%' THEN 'Portal Acesstage'
    WHEN LOWER(categoria) LIKE '%agente de cobran%' THEN 'Agente de cobrança'
    ELSE categoria
END
WHERE LOWER(lancamento) IN (
    'taxa de gestão',
    'taxa de administração',
    'anbima',
    'taxa gestao a pagar',
    'taxa de custódia'
);

UPDATE DW_DESENV.despesas_fundos
SET lancamento = CASE
    WHEN LOWER(lancamento) LIKE '%cvm%' THEN 'CVM'
    WHEN LOWER(lancamento) LIKE '%crdc%' THEN 'CRDC'
    WHEN LOWER(lancamento) LIKE '%custod%' THEN 'Taxa de Custódia'
    WHEN LOWER(lancamento) LIKE '%admin%' THEN 'Taxa de Administração'
    WHEN LOWER(lancamento) LIKE '%gest%' THEN 'Taxa de Gestão'
    WHEN LOWER(lancamento) LIKE '%emiss%' AND LOWER(lancamento) LIKE '%nota%' THEN 'Custo Emissão NC'
    WHEN LOWER(lancamento) LIKE '%anbima%' THEN 'Anbima'
    WHEN LOWER(lancamento) LIKE '%consult%' THEN 'Taxa de Consultoria'
    WHEN LOWER(lancamento) LIKE '%banc%' AND LOWER(lancamento) LIKE '%tarif%' THEN 'Taxa Bancária'
    WHEN LOWER(lancamento) LIKE '%advog%' THEN 'Despesas Jurídicas'
    WHEN LOWER(lancamento) LIKE '%jurid%' THEN 'Despesas Jurídicas'
    WHEN LOWER(lancamento) LIKE '%judic%' THEN 'Despesas Jurídicas'
    WHEN LOWER(lancamento) LIKE '%process%' THEN 'Despesas Jurídicas'
    WHEN LOWER(lancamento) LIKE '%registro%' AND LOWER(lancamento) LIKE '%anbima%' THEN 'Anbima'
    WHEN LOWER(lancamento) LIKE '%acesstage%' THEN 'Portal Acesstage'
    WHEN LOWER(lancamento) LIKE '%agente de cobran%' THEN 'Agente de cobrança'
    ELSE lancamento
END
WHERE LOWER(lancamento) IN (
    'taxa de gestão',
    'taxa de administração',
    'anbima',
    'taxa gestao a pagar',
    'taxa de custódia'
);

-- Remover duplicatas
-- A CTE (Common Table Expression) é usada para identificar duplicatas

WITH CTE_Duplicadas AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY data, valor, nmfundo, lancamento
               ORDER BY DataETL DESC, idcontrole DESC
           ) AS rn
    FROM DW_DESENV.despesas_fundos
)
DELETE FROM DW_DESENV.despesas_fundos
WHERE idcontrole IN (
    SELECT idcontrole
    FROM CTE_Duplicadas
    WHERE rn > 1
);
