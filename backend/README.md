## Correr el backend (MacOS)

Para correr el backend es necesarios primero instalar los requirements en un entrono virtual:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Ahora, para correr todo el backend, desde la carpeta `/Integrated-Multimodal-Data-System`:

```bash
python3 -m backend.app.main
uvicorn backend.app.main:app --reload
```

Pero si es que se esta en la carpeta `/backend` (lo recomendado):


```bash
python3 -m app.main
uvicorn app.main:app --reload
```