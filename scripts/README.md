# Pipeline de Agua - Estrutura em Scripts

## 📁 Organização mínima

Mantivemos apenas o essencial para correr no Colab:

```
scripts/
├── pip_water.py            # Pipeline all-in-one (Colab/local)
└── README.md
```

## 🚀 Como Usar

### Google Colab (prioridade)
```bash
!pip install -r requirements.txt
!python scripts/pip_water.py
```

### Google Colab Browser (upload manual)
```python
from google.colab import files
import os

os.makedirs('/content/PI/scripts', exist_ok=True)
os.makedirs('/content/data/aqualog', exist_ok=True)

print('Upload do pip_water.py')
uploaded_script = files.upload()
for name, content in uploaded_script.items():
	if name == 'pip_water.py':
		with open('/content/PI/scripts/pip_water.py', 'wb') as f:
			f.write(content)

print('Upload dos ficheiros .json')
uploaded_json = files.upload()
for name, content in uploaded_json.items():
	if name.lower().endswith('.json'):
		with open(f'/content/data/aqualog/{name}', 'wb') as f:
			f.write(content)

os.environ['LOCAL_JSON_DIR'] = '/content/data/aqualog'
```

```bash
!python /content/PI/scripts/pip_water.py
```

Se fores usar ficheiros locais no Colab (em vez de API GitHub privada):
```python
import os
os.environ["LOCAL_JSON_DIR"] = "/content/data/aqualog"
```

```bash
!python scripts/pip_water.py
```

## 📌 Ficheiros Principais (fora de scripts/)

- `scripts/pip_water.py` - Pipeline all-in-one para execução
- `.env` - Variáveis de ambiente 
- `requirements.txt` - Dependências Python

