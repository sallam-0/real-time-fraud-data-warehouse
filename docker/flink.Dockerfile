FROM flink:1.18-java11

# Install Python build dependencies + Microsoft ODBC driver for MSSQL
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
    python3-pip \
    python3-dev \
    build-essential \
    curl \
    gnupg2 \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 17 for SQL Server
# Note: dpkg --force-overwrite is needed because the base image's unixodbc-dev
# installs libodbc2/libodbcinst2 which conflict with the Microsoft repo's
# libodbc1/odbcinst1debian2 packages. The overwrite is safe.
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update -qq && \
    ACCEPT_EULA=Y apt-get install -y -qq --no-install-recommends -o Dpkg::Options::="--force-overwrite" msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

# Create python -> python3 symlink
RUN ln -sf /usr/bin/python3 /usr/bin/python

# ---------------------------------------------------------------
# The Flink image bundles pyflink.zip on the PYTHONPATH.
# Zip-imported modules have __file__ = None, which breaks the
# UDF runner (it calls os.path.dirname(pyflink.__file__)).
#
# Fix:  1) Remove the bundled zip
#        2) pip-install the full apache-flink package so that
#           pyflink lives in site-packages with __file__ set
#        3) Also install runtime deps (protobuf, redis)
# ---------------------------------------------------------------

# Remove bundled pyflink.zip that causes __file__ = None
RUN rm -f /opt/flink/opt/python/pyflink.zip 2>/dev/null; \
    find /opt/flink -name 'pyflink.zip' -delete 2>/dev/null; \
    true

# Install full PyFlink (includes bin/ directory for UDF runner)
# Pin to 1.18.1 to match the base Flink image
RUN python3 -m pip install --no-cache-dir \
    'apache-flink==1.18.1' \
    redis \
    pyodbc \
    scikit-learn \
    joblib \
    numpy \
    boto3 \
    pyarrow

# Verify pyflink is importable with __file__ properly set
RUN python3 -c "import pyflink; assert pyflink.__file__ is not None, 'pyflink.__file__ is None!'; print('✓ pyflink installed at:', pyflink.__file__)"

# Verify the bin/ directory exists (needed by UDF runner)
RUN python3 -c "import pyflink, os; bindir = os.path.join(os.path.dirname(pyflink.__file__), 'bin'); assert os.path.isdir(bindir), f'bin dir not found: {bindir}'; print('✓ pyflink bin dir:', bindir)"
