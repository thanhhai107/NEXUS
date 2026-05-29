"""
Download and manage Ollama models.

Usage:
    python -m ingestion.semantic.download_model
    
Commands:
    python -m ingestion.semantic.download_model          # Pull default model
    python -m ingestion.semantic.download_model --list  # List available models
    python -m ingestion.semantic.download_model --pull  # Pull model
    python -m ingestion.semantic.download_model --check  # Check status
"""

from __future__ import annotations

import platform
import subprocess
import sys


# Model configuration
DEFAULT_MODEL = "qwen2.5:0.5b"
ALTERNATIVE_MODELS = {
    "phi3.5-mini": "phi3.5-mini",
    "qwen2.5-1.5b": "qwen2.5:1.5b",
    "qwen2.5-3b": "qwen2.5:3b",
    "smollm3-1.7b": "huggingfacetb/SmolLM3-1.7B-Instruct",
}


def get_system_info() -> str:
    """Get system information."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    if system == "windows":
        return "Windows"
    elif system == "linux":
        return "Linux"
    elif system == "darwin":
        return "macOS"
    
    return f"{system}-{machine}"


def check_ollama_installed() -> bool:
    """Check if Ollama is installed."""
    result = subprocess.run(
        ["ollama", "--version"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def check_ollama_running() -> bool:
    """Check if Ollama service is running."""
    import requests
    try:
        response = requests.get("http://localhost:11434/", timeout=2)
        return response.status_code == 200
    except (OSError, ConnectionError):
        return False


def get_installed_models() -> list[dict]:
    """Get list of installed models."""
    result = subprocess.run(
        ["ollama", "list"],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        return []
    
    lines = result.stdout.strip().split("\n")
    if len(lines) <= 1:
        return []
    
    models = []
    for line in lines[1:]:  # Skip header
        parts = line.split()
        if parts:
            models.append({
                "name": parts[0],
                "size": parts[1] if len(parts) > 1 else "unknown",
                "modified": parts[2] if len(parts) > 2 else "",
            })
    
    return models


def pull_model(model: str = DEFAULT_MODEL) -> bool:
    """Pull a model from Ollama."""
    print(f"Pulling model: {model}")
    print("(This may take a few minutes on first run)\n")
    
    result = subprocess.run(
        ["ollama", "pull", model],
    )
    
    return result.returncode == 0


def start_ollama() -> bool:
    """Start Ollama service."""
    system = platform.system().lower()
    
    if system == "windows":
        # On Windows, Ollama runs as a service automatically
        print("Ollama should be running. Start it manually if needed.")
        return True
    elif system == "linux" or system == "darwin":
        result = subprocess.run(
            ["launchctl", "start", "ollama"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    
    return False


def main():
    """Main entry point."""
    print("=" * 60)
    print("NEXUS Ollama Model Manager")
    print("=" * 60)
    print(f"\nSystem: {get_system_info()}")
    
    # Check if Ollama is installed
    if not check_ollama_installed():
        print("\n❌ Ollama is not installed!")
        print("\nPlease install Ollama:")
        print("  1. Go to https://ollama.com/download")
        print("  2. Download and install for your OS")
        print("  3. Restart your terminal")
        sys.exit(1)
    
    # Check if Ollama is running
    running = check_ollama_running()
    print("\nOllama installed: ✅")
    print(f"Ollama running:    {'✅' if running else '❌'}")
    
    # Parse arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        
        if arg == "--check":
            # Check status only
            if running:
                models = get_installed_models()
                print(f"\nInstalled models ({len(models)}):")
                for m in models:
                    print(f"  - {m['name']} ({m['size']})")
            else:
                print("\nStart Ollama:")
                if platform.system().lower() == "windows":
                    print("  - Search for 'Ollama' in Start Menu and run it")
                    print("  - Or run: ollama serve")
                else:
                    print("  - Run: ollama serve")
            return
        
        elif arg == "--list":
            # List models
            if not running:
                print("\n⚠️  Ollama is not running. Start it first.")
                return
            
            models = get_installed_models()
            print(f"\nInstalled models ({len(models)}):")
            if models:
                for m in models:
                    print(f"  - {m['name']} ({m['size']})")
            else:
                print("  No models installed yet.")
            
            print("\nAvailable models to pull:")
            print(f"  - {DEFAULT_MODEL} (recommended)")
            for name, model in ALTERNATIVE_MODELS.items():
                print(f"  - {model} ({name})")
            return
        
        elif arg == "--pull":
            # Pull model
            model = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_MODEL
            if pull_model(model):
                print(f"\n✅ Model {model} installed successfully!")
            else:
                print(f"\n❌ Failed to pull model {model}")
                sys.exit(1)
            return
    
    # Default: check status and offer to pull model
    if not running:
        print("\n⚠️  Ollama is not running!")
        print("\nStart Ollama:")
        if platform.system().lower() == "windows":
            print("  - Search for 'Ollama' in Start Menu and click it")
            print("  - Or open terminal and run: ollama serve")
        else:
            print("  - Run: ollama serve")
        print()
    
    # Check if default model is installed
    models = get_installed_models()
    model_names = [m["name"] for m in models]
    
    if DEFAULT_MODEL in model_names:
        print(f"\n✅ Model {DEFAULT_MODEL} is installed!")
    else:
        print(f"\n❌ Model {DEFAULT_MODEL} is not installed!")
        print("\nPull it now? (y/n)")
        
        if len(sys.argv) > 1 and sys.argv[1] == "--yes":
            response = "y"
        else:
            response = input("> ").strip().lower()
        
        if response == "y":
            if pull_model(DEFAULT_MODEL):
                print(f"\n✅ Model {DEFAULT_MODEL} installed successfully!")
            else:
                print("\n❌ Failed to pull model")
                sys.exit(1)


if __name__ == "__main__":
    main()
