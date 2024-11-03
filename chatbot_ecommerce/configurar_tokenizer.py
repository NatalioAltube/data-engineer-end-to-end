from transformers import GPT2Tokenizer

# Especifica el directorio donde guardaste el modelo fine-tuneado
checkpoint_path = "C:/Users/natal/OneDrive/Escritorio/data_engineer_proyects/chatbot_ecommerce/results/checkpoint-21"

# Descargar el tokenizer original de GPT-2 y guardarlo en el directorio de checkpoint
tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
tokenizer.save_pretrained(checkpoint_path)