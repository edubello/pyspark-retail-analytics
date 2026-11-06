"""
Downloader de Dados Brutos (Raw Data Downloader)
Projeto: Retail Analytics Lakehouse (PySpark & Delta Lake)
Autor: Eduardo Bello

Baixa os arquivos de exemplo (sales.csv, users.csv e products.json) 
diretamente do repositório remoto para a camada Raw local.
"""

from pathlib import Path
import urllib.request
import yaml


def load_config() -> dict:
    """Carrega o arquivo de configuração settings.yaml."""
    config_path = Path("config/settings.yaml") if Path("config/settings.yaml").exists() else Path("../config/settings.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def download_raw_data() -> None:
    """Faz o download dos arquivos brutos da web para a pasta raw."""
    settings = load_config()
    raw_dir = Path(settings["paths"]["raw_data"])
    
    # Garante que o diretório raw existe
    raw_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Preparando diretório Raw: {raw_dir}")

    # URL base do repositório de dados de teste
    BASE_URL = "https://raw.githubusercontent.com/KaelDucatti/pyspark_test_data/main"
    
    files_to_download = ["products.json", "sales.csv", "users.csv"]

    for file_name in files_to_download:
        file_url = f"{BASE_URL}/{file_name}"
        dest_path = raw_dir / file_name
        
        print(f"[INFO] Baixando {file_name} de {file_url}...")
        try:
            urllib.request.urlretrieve(file_url, str(dest_path))
            print(f"[SUCCESS] {file_name} baixado com sucesso em {dest_path}")
        except Exception as e:
            print(f"[ERROR] Falha ao baixar {file_name}: {e}")


if __name__ == "__main__":
    download_raw_data()