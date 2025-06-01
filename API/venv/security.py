from fastapi import Header, HTTPException
import os
from dotenv import load_dotenv

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
parent_dir = os.path.dirname(parent_dir)
env_path = os.path.join(parent_dir,".env")
load_dotenv(env_path)
API_KEY = os.getenv("API_KEY")

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")