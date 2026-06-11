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


_ENV = _load_env_file()


def env(name: str, default: str) -> str:
    return _ENV.get(name, default)


DB_CONFIG = {
    "host": env("DB_HOST", "127.0.0.1"),
    "port": int(env("DB_PORT", "3306")),
    "user": env("DB_USER", "root"),
    "password": env("DB_PASSWORD", ""),
    "database": env("DB_NAME", "geospatial_routing_system"),
}

REDIS_CONFIG = {
    "host": env("REDIS_HOST", "127.0.0.1"),
    "port": int(env("REDIS_PORT", "6379")),
    "db": int(env("REDIS_DB", "0")),
    "decode_responses": True,
}
