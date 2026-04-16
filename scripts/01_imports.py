"""
Celula 1: Imports e Configuracao de Environment
"""

import os
import json
from dotenv import load_dotenv
from water_pipeline import run_pipeline

# Carrega variaveis do ficheiro .env
load_dotenv()

# Re-exporta para uso em outros scripts
__all__ = ["os", "json", "load_dotenv", "run_pipeline"]
