"""
Celula 5: Instruções para Deploy em Flask
"""


def print_flask_instructions():
    """Imprime instruções para executar em Flask"""
    
    instructions = """
=== Para correr a Pipeline em Flask (Local/Production) ===

Passo 1: Instalar dependencias
    pip install -r requirements.txt

Passo 2: Iniciar o servidor Flask
    python app.py

Passo 3: Chamar a Pipeline via HTTP
    POST http://127.0.0.1:5000/run
    
    Ou usar curl:
    curl -X POST http://127.0.0.1:5000/run

Passo 4: Ver health status
    GET http://127.0.0.1:5000/health
    
    Ou usar curl:
    curl http://127.0.0.1:5000/health

Notas:
- O servidor por default corre em http://127.0.0.1:5000
- A mesma logica da pipeline (water_pipeline.py) é usada tanto na Flask como aqui
- A configuracao vem do ficheiro .env
"""
    print(instructions)


if __name__ == "__main__":
    print_flask_instructions()
