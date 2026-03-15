FROM apache/airflow:2.8.1-python3.11

USER root

# Install Microsoft ODBC Driver 18 for SQL Server
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc -o /tmp/microsoft.asc \
    && rm -f /usr/share/keyrings/microsoft-prod.gpg \
    && gpg --batch --yes --dearmor -o /usr/share/keyrings/microsoft-prod.gpg /tmp/microsoft.asc \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev \
    && rm -f /tmp/microsoft.asc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
