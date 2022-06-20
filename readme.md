# Aplicação
- Monitoramento de fluxo de trabalho com Airflow

# Ferramentas
- Airflow
- Python
- Aws
- Data lake
- Bucket S3
- Pandas

# Funcionamento do fluxo
- Verificar se a api é valida
- Busca todos os dados da api
- Processa os dados no Data Frame do pandas e armazena em um arquivo .parquet
- Faz o upload do arquivo para o Data Lake
- Remove o arquivo do DF local para evitar duplicidade de dados