-- Dev-only Honcho v3 validation database bootstrap.
-- Keep this separate from the production/v2 Honcho volume.
CREATE EXTENSION IF NOT EXISTS vector;

SELECT 'CREATE DATABASE litellm'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'litellm')\gexec
