# PIP Water

Pipeline para ingestão, limpeza, persistência e relatório de dados de água, com suporte para:

- descoberta de ficheiros JSON no GitHub, local ou por URL direta
- persistência em MongoDB, TiDB e CrateDB
- deteção de anomalias comparando o lote atual com os últimos 2 dias de histórico no TiDB
- resumo por email com tabela de execução e tabela de anomalias

## Estrutura

```text
app.py           # Flask app (UI + /run assíncrono + /status)
pipeline.py      # fachada/orquestração da pipeline
config.py        # configuração central e contexto de execução
env_utils.py     # helpers de runtime/env/segredos
data_parser.py   # descoberta, parsing, limpeza e anomalias
db_writer.py     # persistência MongoDB / TiDB / CrateDB
notifier.py      # envio de email e rendering HTML
pi_water.ipynb   # notebook Colab (clona o repo e corre via `import app`)
scripts/
	pip_water.py   # entrypoint de compatibilidade (legacy shim)
localINPUTS/
	test_anomaly_over.json   # ficheiro de teste (anomalia por excesso)
	test_anomaly_under.json  # ficheiro de teste (anomalia por defeito)
output_water/
	water_data_raw.json
	water_data_clean.csv
	water_anomaly_report.json
	water_result.json
```

## Dependências

```bash
pip install -r requirements.txt
```

## Configuração

As credenciais e opções principais podem ser definidas em `.env` ou em variáveis de ambiente:

- `GITHUB_TOKEN`
- `JSON_FILE_URLS`
- `LOCAL_JSON_DIR`
- `REPO_JSON_FILES`
- `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION`
- `TIDB_HOST`, `TIDB_PORT`, `TIDB_USER`, `TIDB_PASSWORD`, `TIDB_DATABASE`, `TIDB_TABLE`, `TIDB_CA_PATH`
- `CRATEDB_HOST`, `CRATEDB_PORT`, `CRATEDB_USER`, `CRATEDB_PASSWORD`, `CRATEDB_DATABASE`, `CRATEDB_TABLE`, `CRATEDB_SSLMODE`
- `EMAIL_ENABLED`, `EMAIL_BACKEND`, `EMAIL_FROM`, `EMAIL_TO`, `EMAIL_USERNAME`, `EMAIL_PASSWORD`
- `BREVO_API_KEY`, `BREVO_SENDER_NAME`, `BREVO_SENDER_EMAIL`
- `ANOMALY_THRESHOLD_RATIO`, `ANOMALY_LOOKBACK_DAYS` e `REPO_SAMPLE_SIZE`

## Como executar

### Google Colab

1. Abre [pi_water.ipynb](pi_water.ipynb) no Google Colab.
2. Configura os Colab Secrets necessários (ver tabela no notebook).
3. Executa a célula Python — ela clona o repo, instala os requisitos e corre a pipeline.

### Local (Flask / linha de comandos)

### Script (CLI)

```bash
python scripts/pip_water.py
```

O script mantém compatibilidade com o comportamento antigo, mas agora delega na arquitetura modular (`pipeline.py`, `data_parser.py`, `db_writer.py`, `notifier.py`).

Smoke test recomendado (sem writes em DB):

```bash
SKIP_DB_WRITES=true EMAIL_ENABLED=false python scripts/pip_water.py
```

No Windows PowerShell:

```powershell
$env:SKIP_DB_WRITES='true'; $env:EMAIL_ENABLED='false'; python scripts/pip_water.py
```

Isto valida descoberta/parsing/transform/anomalias/outputs sem tocar em MongoDB, TiDB, CrateDB nem email.

Por defeito, quando a origem é o repositório, a pipeline escolhe 30 ficheiros aleatórios antes de os ler. Podes alterar isso com `REPO_SAMPLE_SIZE`.

### Flask API

```bash
python app.py
```

Endpoints disponíveis:

- `GET /health` - valida se o serviço está ativo
- `GET /config` - mostra configuração ativa sem segredos
- `POST /run` - executa a pipeline uma vez e devolve o resultado JSON

Exemplos de teste:

```bash
curl http://127.0.0.1:5000/health

curl http://127.0.0.1:5000/config

run:
curl.exe -X POST http://127.0.0.1:5000/run

```

Tambem podes controlar o comportamento default com `RUN_SEND_EMAIL_DEFAULT=true|false`.

### Deploy no Render

Este projeto já inclui `render.yaml`, por isso podes criar um `Web Service` no Render apontando para o repositório e usar:

- build: `pip install -r requirements.txt`
- start: `gunicorn app:app`

Configura as variáveis de ambiente no Render da mesma forma que no `.env` local, sobretudo:

- `GITHUB_TOKEN`
- `JSON_FILE_URLS`, `LOCAL_JSON_DIR` ou `REPO_JSON_FILES`
- `EMAIL_ENABLED`, `EMAIL_FROM`, `EMAIL_TO`, `EMAIL_USERNAME`, `EMAIL_PASSWORD`
- `MONGO_URI`, `TIDB_*`, `CRATEDB_*` se fores persistir na cloud

No Render, o serviço usa a variável `PORT` automaticamente; a app já está preparada para isso.

### Email no Render com Brevo

O Render bloqueia SMTP tradicional em várias portas. Para o deploy no Render, usa o backend `brevo` por API HTTPS:

- `EMAIL_BACKEND=brevo`
- `BREVO_API_KEY`
- `BREVO_SENDER_NAME`
- `BREVO_SENDER_EMAIL`

Podes continuar a usar `EMAIL_TO` como lista de destinatários. Em modo local, podes deixar `EMAIL_BACKEND=smtp`.

O email enviado pela pipeline inclui agora o ambiente detetado no assunto e no corpo, por exemplo: `google-colab`, `local`, `flask` ou `render`.

## Como detetar anomalias

Para validar a deteção de anomalias com dados de teste:

1. Executa `scripts/generate_test_anomaly.py` ou a célula de teste no notebook.
2. Confirma que o ficheiro é guardado em `output_water/test_anomaly_sensors.json`.
3. Define `LOCAL_JSON_DIR` para uma pasta que contenha esse ficheiro, ou adiciona-o à lista de descoberta.
4. Reexecuta a pipeline.
5. Se o histórico no TiDB existir, o email e o `water_anomaly_report.json` vão incluir a tabela das anomalias.

### Exemplo rápido no notebook

```python
CONFIG['local_json_dir'] = 'test_data_anomaly'
CONFIG['json_file_urls'] = ''
CONFIG['repo_json_files'] = ''
```

Depois volta a correr a célula principal da pipeline.

## Outputs

- `output_water/water_data_raw.json` - dados normalizados da execução
- `output_water/water_data_clean.csv` - exportação tabular limpa
- `output_water/water_anomaly_report.json` - resumo da deteção de anomalias
- `output_water/water_result.json` - resultado final da pipeline com métricas e estado das persistências

## Notas

- O notebook continua a ser a referência principal.
- O script `scripts/pip_water.py` é um shim de compatibilidade para não quebrar imports legados.
- A app Flask importa agora a fachada modular em `pipeline.py`.
- O email inclui uma tabela de anomalias quando `anomalous_counters > 0`.

