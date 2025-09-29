import os
from dotenv import load_dotenv
from alembic import context
from sqlalchemy import engine_from_config, pool

load_dotenv()

config = context.config
# .env에 있는 DSN 불러오기
target_url = os.getenv("PG_DSN")

def run_migrations_offline():
    context.configure(
        url=target_url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(
        {"sqlalchemy.url": target_url},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()