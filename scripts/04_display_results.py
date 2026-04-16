"""
Celula 4: Display dos Resultados em Pandas DataFrame
"""

import sys
import pandas as pd
from pathlib import Path

# Garante imports de modulos pai
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def display_results(result):
    """Mostra os resultados da pipeline em formato DataFrame"""
    
    if result.get("status") == "ok":
        csv_path = result["output"]["csv_path"]
        print(f"\n=== Pipeline Concluida com Sucesso ===")
        print(f"Output CSV: {csv_path}\n")
        
        df = pd.read_csv(csv_path)
        print(f"Shape: {df.shape}")
        print(f"\nPrimeiras linhas:")
        print(df.head())
        
        return df
    else:
        print(f"\n=== Pipeline Falhou ===")
        print(f"Erro: {result.get('error')}")
        print(f"Hint: {result.get('hint', 'N/A')}")
        return None


if __name__ == "__main__":
    # Para testes (requires que 03_run_pipeline.py tenha sido executado)
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_pipeline_script", str(project_root / "scripts" / "03_run_pipeline.py"))
    script_03 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script_03)
    
    result = script_03.execute_pipeline()
    df = display_results(result)
