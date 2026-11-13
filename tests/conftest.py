import os
from urllib.parse import urlsplit

os.environ.setdefault("API_KEY", "local-demo-change-me")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:55432/payments_test")
if not urlsplit(os.environ["DATABASE_URL"]).path.endswith("_test"):
    raise RuntimeError("Tests require a separate database ending in _test")
