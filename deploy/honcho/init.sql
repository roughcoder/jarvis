-- Honcho requires pgvector. Mounted into the Postgres init dir so the
-- extension exists before Honcho runs its migrations (provision_db.py).
CREATE EXTENSION IF NOT EXISTS vector;
