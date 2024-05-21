import asyncio
import logging
import sys
import os
from dotenv import load_dotenv
from bot import start_bot

load_dotenv()

TOKEN = os.getenv("TOKEN")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(start_bot(TOKEN))
