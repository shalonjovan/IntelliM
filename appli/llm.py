import os
from dotenv import load_dotenv
from google import genai

# Load env HERE too
load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise RuntimeError("GEMINI_API_KEY not loaded")

client = genai.Client(api_key=API_KEY)

def chat_with_gemini(messages: list[str]) -> str:
    prompt = "\n".join(messages)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return response.text
