import os
from dotenv import load_dotenv

load_dotenv()


def get_openai_api_key():
    return os.getenv("openai_api_key")
