# Pipeline de Agua - Estrutura em Scripts

## 📁 Organização (como os teus colegas fazem)

Cada célula do notebook original agora está num ficheiro separado dentro da pasta `scripts/`:

```
scripts/
├── 01_imports.py           # Importações e setup de environment
├── 02_config.py            # Configuração da pipeline (repo, output, etc)
├── 03_run_pipeline.py      # Executar a pipeline
├── 04_display_results.py   # Visualizar resultados em DataFrame
├── 05_flask_info.py        # Instruções para Flask
├── main.py                 # Orquestra tudo (entry point)
└── __init__.py             # Torna pasta em Python package
```

## 🚀 Como Usar

### Opção 1: Executar Completo
```bash
python scripts/main.py
```
Isto executa todos os passos: config → pipeline → display → info

### Opção 2: Executar Passos Individuais
```bash
# Só ver configuração
python scripts/02_config.py

# Só correr a pipeline
python scripts/03_run_pipeline.py

# Só visualizar resultados
python scripts/04_display_results.py

# Ver instruções Flask
python scripts/05_flask_info.py
```

## 📝 Vantagens desta Estrutura

✅ **Modularidade**: Cada ficheiro tem responsabilidade única  
✅ **Reutilização**: Pode importar cada script/função isoladamente  
✅ **Legibilidade**: Código melhor organizado e documentado  
✅ **Facilita Debugging**: Pode executar passos individualmente  
✅ **Bom para Controle de Versão**: Mudanças isoladas por funcionalidade  

## 🔧 Importar em Outro Projeto

Se quiseres importar a pipeline de outro ficheiro:

```python
import sys
sys.path.insert(0, './scripts')

from water_pipeline import run_pipeline
from scripts.scripts_02_config import setup_config

config = setup_config()
result = run_pipeline()
```

## 📌 Ficheiros Principais (fora de scripts/)

- `water_pipeline.py` - O módulo principal com toda a lógica
- `app.py` - Flask API wrapper
- `.env` - Variáveis de ambiente (⚠️ NUNCA fazer commit)
- `.env.example` - Template (para distribuir)
- `requirements.txt` - Dependências Python

## 🐛 Troubleshooting

Se vires erro `ModuleNotFoundError: No module named 'water_pipeline'`:
- Certifica-te que estás a correr de dentro da pasta raiz do projeto
- Ou adiciona o path manualmente: `sys.path.insert(0, '.')`
