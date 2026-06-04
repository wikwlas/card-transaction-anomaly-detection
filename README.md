Ten projekt używa [uv](https://github.com/astral-sh/uv) do ultra-szybkiego zarządzania zależnościami.

1. Zainstaluj `uv` (jeśli jeszcze nie masz):
   `curl -LsSf https://astral.sh/uv/install.sh | sh`
   *(Lub na Windows: `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`)*

2. Pobierz projekt i zsynchronizuj środowisko:
   ```bash
   uv sync


