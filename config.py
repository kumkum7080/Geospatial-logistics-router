from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"


def _load_env_file(path: Path = ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


import os

_ENV = _load_env_file()


def env(name: str, default: str) -> str:
    if name in os.environ:
        return os.environ[name]
    return _ENV.get(name, default)


DB_CONFIG = {
    "host": env("DB_HOST", env("MYSQLHOST", "127.0.0.1")),
    "port": int(env("DB_PORT", env("MYSQLPORT", "3306"))),
    "user": env("DB_USER", env("MYSQLUSER", "root")),
    "password": env("DB_PASSWORD", env("MYSQLPASSWORD", "")),
    "database": env("DB_NAME", env("MYSQLDATABASE", "geospatial_routing_system")),
}

REDIS_CONFIG = {
    "host": env("REDIS_HOST", env("REDISHOST", "127.0.0.1")),
    "port": int(env("REDIS_PORT", env("REDISPORT", "6379"))),
    "db": int(env("REDIS_DB", "0")),
    "decode_responses": True,
}
