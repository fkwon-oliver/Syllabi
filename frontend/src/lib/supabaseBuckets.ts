export const BUCKETS = {
  DOCS: (process.env.NEXT_PUBLIC_SUPABASE_BUCKET_DOCS as string) || 'documents',
  CHAT_FILES: (process.env.NEXT_PUBLIC_SUPABASE_BUCKET_CHAT_FILES as string) || 'chat-files',
  ASSETS: (process.env.NEXT_PUBLIC_SUPABASE_BUCKET_ASSETS as string) || 'chatbot-assets',
} as const;
