#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE ${AIRFLOW_DB};
    CREATE DATABASE ${PROCESSING_DB};

    -- Create user and password airflow_db 
    CREATE USER ${AIRFLOW_USER:-airflow} WITH ENCRYPTED PASSWORD '${AIRFLOW_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB} TO airflow;

    -- Create user and password processing_db 
    CREATE USER ${PROCESSING_USER:-processing_user} WITH ENCRYPTED PASSWORD '${PROCESSING_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE ${PROCESSING_DB} TO processing_user;
EOSQL