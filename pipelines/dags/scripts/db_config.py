import os

# ---------------------------------------------------------------------------
# Docker secrets helper — reads the .env secret from /run/secrets/env_file
# ---------------------------------------------------------------------------
_DOCKER_SECRET_PATH = "/run/secrets/env_file"
_secrets_cache = None

def _load_docker_secrets() -> dict:
    """Parse the Docker secret file (mounted .env) into a dict of key=value pairs."""
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache

    _secrets_cache = {}
    if not os.path.isfile(_DOCKER_SECRET_PATH):
        return _secrets_cache

    try:
        with open(_DOCKER_SECRET_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    _secrets_cache[key.strip()] = value.strip()
    except Exception:
        pass
    return _secrets_cache

def _clean_value(value):
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1].strip()
    return value

def _is_set(value) -> bool:
    return value is not None and str(value).strip() != ""

try:
    from airflow.models import Variable
    _HAS_AIRFLOW = True
except Exception:
    Variable = None
    _HAS_AIRFLOW = False

def _get(key, default=None, aliases=None):
    """Resolve a config value with priority: Airflow -> Docker Secret -> Env -> Default"""
    keys = [key] + (aliases or [])

    # 1) Try Airflow Variables
    if _HAS_AIRFLOW:
        for k in keys:
            try:
                val = Variable.get(k, default_var=None)
                if _is_set(val):
                    return _clean_value(val)
            except Exception:
                pass

    # 2) Try Docker Secrets
    secrets = _load_docker_secrets()
    for k in keys:
        val = secrets.get(k)
        if _is_set(val):
            return _clean_value(val)

    # 3) Try OS Env Vars
    for k in keys:
        val = os.getenv(k)
        if _is_set(val):
            return _clean_value(val)

    # 4) Fallback
    return default

def get_mssql_conn_str() -> str:
    """Build MSSQL connection string from config sources."""
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={_get('MSSQL_HOST', 'host.docker.internal')},{_get('MSSQL_PORT', '1433')};"
        f"DATABASE={_get('MSSQL_DATABASE', 'BankingSystem', aliases=['MSSQL_DB'])};"
        f"UID={_get('MSSQL_USER', 'sa')};"
        f"PWD={_get('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=Optional;"
    )

def get_snowflake_cfg() -> dict:
    """Build Snowflake config dict from config sources."""
    return {
        "account":   _get("SNOWFLAKE_ACCOUNT",  "AGREMWF-RQ59577", aliases=["DBT_SNOWFLAKE_ACCOUNT"]),
        "user":      _get("SNOWFLAKE_USER",      "SALLAM", aliases=["DBT_SNOWFLAKE_USER"]),
        "password":  _get("SNOWFLAKE_PASSWORD", aliases=["SNOWFLAKE_PASS", "SNOWFLAKE_PWD", "DBT_SNOWFLAKE_PASSWORD"]),
        "database":  _get("SNOWFLAKE_DATABASE",  "FRAUD_DWH", aliases=["SNOWFLAKE_DB", "DBT_SNOWFLAKE_DATABASE"]),
        "schema":    "STAGING",
        "warehouse": _get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH", aliases=["DBT_SNOWFLAKE_WAREHOUSE"]),
        "role":      _get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN", aliases=["DBT_SNOWFLAKE_ROLE"]),
    }

