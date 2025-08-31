import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+asyncpg://{os.getenv('PG_USER','postgres')}:{os.getenv('PG_PASSWORD','')}@{os.getenv('PG_HOST','127.0.0.1')}:{os.getenv('PG_PORT','5432')}/{os.getenv('PG_DATABASE','music_app')}"
)

engine: AsyncEngine = create_async_engine(DATABASE_URL, future=True, echo=False)

async def get_connection() -> AsyncConnection:
    return await engine.connect()
    