"""
Celula 3: Executar a Pipeline
"""

import sys
import json
from pathlib import Path

# Garante imports de modulos pai
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from water_pipeline import run_pipeline

def setup_config():
    """Importa a config de 02_config.py"""
    import importlib.util
    spec = importlib.util.spec_from_file_location("config", str(project_root / "scripts" / "02_config.py"))
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    return config_module.setup_config()


def execute_pipeline():
    """Executa a pipeline e retorna o resultado"""
    
    # Garante que a configuracao esta setup
    setup_config()
    
    # Executa a pipeline
    result = run_pipeline()
    
    return result


if __name__ == "__main__":
    print("=== Executando Pipeline ===\n")
    result = execute_pipeline()
    print(json.dumps(result, indent=2, ensure_ascii=False))
