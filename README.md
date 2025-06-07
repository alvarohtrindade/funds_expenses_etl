# ETL de Despesas de Fundos

Sistema ETL para processamento de despesas de fundos de investimento de diferentes custodiantes.

## Visão Geral

Este projeto implementa um sistema ETL (Extract, Transform, Load) para processar arquivos de despesas de fundos de investimento de diferentes custodiantes, padronizando os lançamentos e valores. O sistema pode processar arquivos de diferentes formatos e estruturas, e gerar um arquivo consolidado com as despesas normalizadas.

Os custodiantes suportados atualmente são:
- BTG
- Daycoval
- Master

## Estrutura do Projeto

```
fund_expenses_etl/
│
├── config/                      # Configurações
│   ├── map_files.json          # Configurações para leitura dos arquivos
│   └── map_lancamentos.json    # Mapeamento para padronização dos lançamentos
│
├── data/                        # Diretório para arquivos de dados
│   ├── input/                   # Arquivos de entrada
│   └── output/                  # Arquivos de saída e logs
│
├── src/                         # Código-fonte
│   ├── etl/                     # Componentes do ETL
│   │   ├── extractors/          # Extratores para diferentes custodiantes
│   │   ├── transformers/        # Transformadores de dados
│   │   └── loaders/             # Carregadores de dados
│   ├── utils/                   # Utilitários
│   └── main.py                  # Ponto de entrada principal
│
├── tests/                       # Testes unitários
├── README.md                    # Este arquivo
├── requirements.txt             # Dependências do projeto
└── setup.py                     # Script de instalação
```

## Requisitos

- Python 3.8 ou superior
- Dependências listadas em `requirements.txt`

## Instalação

### Usando pip

```bash
# Instalar diretamente do repositório
pip install git+https://github.com/seu-usuario/fund_expenses_etl.git

# Ou, após clonar o repositório
git clone https://github.com/seu-usuario/fund_expenses_etl.git
cd fund_expenses_etl
pip install -e .
```

### Instalação manual

```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/fund_expenses_etl.git
cd fund_expenses_etl

# Instalar dependências
pip install -r requirements.txt
```

## Uso

O sistema oferece uma interface de linha de comando (CLI) para fácil utilização.

### Processamento de Arquivos

Para processar arquivos de despesas e gerar um arquivo consolidado:

```bash
# Usando o comando instalado
fund-etl process --input-dir /caminho/para/arquivos --output-dir /caminho/para/saida

# Ou executando diretamente
python src/main.py process --input-dir /caminho/para/arquivos --output-dir /caminho/para/saida
```

Opções disponíveis:
- `--input-dir`, `-i`: Diretório com os arquivos de entrada (obrigatório)
- `--output-dir`, `-o`: Diretório para os arquivos de saída (obrigatório)
- `--pattern`, `-p`: Padrão para filtrar arquivos (glob) (padrão: `*.*`)
- `--custodiante`, `-c`: Custodiante específico a processar
- `--recursive`, `-r`: Processar subdiretórios recursivamente
- `--format`, `-f`: Formato do arquivo de saída (excel, csv, sqlite, parquet) (padrão: excel)

### Análise de Arquivos

Para analisar um arquivo sem processá-lo completamente:

```bash
# Usando o comando instalado
fund-etl analyze /caminho/para/arquivo.csv

# Ou executando diretamente
python src/main.py analyze /caminho/para/arquivo.csv
```

Opções disponíveis:
- `--custodiante`, `-c`: Custodiante do arquivo (opcional)

## Configuração

O sistema utiliza dois arquivos de configuração JSON:

### map_files.json

Define configurações para leitura de arquivos de diferentes custodiantes, mapeamento de colunas, validações e regras de categorização.

### map_lancamentos.json

Define o mapeamento para padronização dos nomes de lançamentos, permitindo que diferentes descrições de lançamentos sejam normalizadas para um formato padrão.

## Contribuição

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Faça commit das alterações (`git commit -am 'Adiciona nova feature'`)
4. Faça push para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.