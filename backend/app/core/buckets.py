import os

DOCS = os.getenv("SUPABASE_BUCKET_DOCS", "documents")
CHAT_FILES = os.getenv("SUPABASE_BUCKET_CHAT_FILES", "chat-files")
ASSETS = os.getenv("SUPABASE_BUCKET_ASSETS", "chatbot-assets")
