"""
Celula 2: Configuracao da Pipeline (repo, output folders, etc)
"""

import os

def setup_config():
    """Define as variaveis de ambiente default para a pipeline"""
    
    # Configuracao base (podes alterar no .env ou aqui)
    os.environ.setdefault("REPO_OWNER", "pedroccpimenta")
    os.environ.setdefault("REPO_NAME", "datafiles")
    os.environ.setdefault("REPO_FOLDER", "aqualog")
    os.environ.setdefault("REPO_BRANCH", "master")
    os.environ.setdefault("OUTPUT_FOLDER", "./output_water")
    os.environ.setdefault("REQUEST_TIMEOUT", "30")
    
    # Se o repo for privado, podes definir token manualmente:
    # os.environ["GITHUB_TOKEN"] = "ghp_..."
    
    # Alternativa: links raw diretos separados por virgula
    # os.environ["JSON_FILE_URLS"] = "https://raw.githubusercontent.com/.../a.json,https://raw.githubusercontent.com/.../b.json"
    
    config = {
        "repo": f"{os.environ.get('REPO_OWNER')}/{os.environ.get('REPO_NAME')}",
        "folder": os.environ.get("REPO_FOLDER"),
        "branch": os.environ.get("REPO_BRANCH"),
        "output_folder": os.environ.get("OUTPUT_FOLDER"),
        "has_github_token": bool(os.environ.get("GITHUB_TOKEN")),
        "json_file_urls": os.environ.get("JSON_FILE_URLS", ""),
    }
    
    return config


if __name__ == "__main__":
    config = setup_config()
    print("=== Configuracao da Pipeline ===")
    print(f"Repo: {config['repo']}")
    print(f"Folder: {config['folder']}")
    print(f"Branch: {config['branch']}")
    print(f"Output Folder: {config['output_folder']}")
    print(f"Tem GITHUB_TOKEN? {config['has_github_token']}")
    print(f"JSON_FILE_URLS: {repr(config['json_file_urls'])}")
