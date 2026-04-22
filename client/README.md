# igem

Python client for the IGEM API.

## Install

```bash
pip install igem
```

## Usage

```python
from igem import IgemClient

with IgemClient(base_url="http://localhost:8000") as client:
    print(client.health())
```
