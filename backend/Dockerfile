FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry && poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* ./
RUN poetry install --only main --no-interaction

COPY src ./src

CMD ["uvicorn", "igem_backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
