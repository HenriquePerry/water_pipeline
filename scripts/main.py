"""
Main Script: Orquestra a execução de todos os passos da Pipeline

Uso:
    python scripts/main.py
"""

import sys
import os as os_module
from pathlib import Path

# Garante que o diretorio pai esta no path para imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Chama modulos do scripts
import importlib.util

# Helper para importar modulos dinamicamente
def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

# Carrega os scripts em ordem
script_01 = load_module("script_01", project_root / "scripts" / "01_imports.py")
script_02 = load_module("script_02", project_root / "scripts" / "02_config.py")
script_03 = load_module("script_03", project_root / "scripts" / "03_run_pipeline.py")
script_04 = load_module("script_04", project_root / "scripts" / "04_display_results.py")
script_05 = load_module("script_05", project_root / "scripts" / "05_flask_info.py")

os = os_module
json = script_01.json
load_dotenv = script_01.load_dotenv
run_pipeline = script_01.run_pipeline
setup_config = script_02.setup_config
execute_pipeline = script_03.execute_pipeline
display_results = script_04.display_results
print_flask_instructions = script_05.print_flask_instructions


def main():
    """Executa o workflow completo da pipeline"""
    
    print("=" * 60)
    print("PIPELINE DE AGUA - WORKFLOW COMPLETO")
    print("=" * 60)
    
    # Passo 1: Configuracao
    print("\n[1/4] Carregando Configuracao...")
    config = setup_config()
    print(f"✓ Repo: {config['repo']} / {config['folder']} ({config['branch']})")
    print(f"✓ Output: {config['output_folder']}")
    print(f"✓ GitHub Token presente: {config['has_github_token']}")
    
    # Passo 2: Executar Pipeline
    print("\n[2/4] Executando Pipeline...")
    result = execute_pipeline()
    
    if result.get("status") != "ok":
        print(f"✗ Pipeline falhou: {result.get('error')}")
        print(f"  Dica: {result.get('hint', 'Verifica o .env')}")
        return
    
    print(f"✓ Pipeline concluida com sucesso (run_id: {result.get('run_id')})")
    
    # Passo 3: Visualizar Resultados
    print("\n[3/4] Exibindo Resultados...")
    df = display_results(result)
    
    if df is not None:
        print(f"✓ Dados carregados: {len(df)} records")
    
    # Passo 4: Info sobre Flask
    print("\n[4/4] Informacoes de Deployment...")
    print_flask_instructions()
    
    print("\n" + "=" * 60)
    print("WORKFLOW CONCLUIDO")
    print("=" * 60)


if __name__ == "__main__":
    main()
