import os
import pandas as pd
import numpy as np
import faiss
import torch
from transformers import GPT2Tokenizer, GPT2LMHeadModel
import gradio as gr
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.faiss import FAISS
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain.schema import Document

# Cargar variables desde el archivo .env
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

# Ruta al checkpoint del modelo fine-tuneado
MODEL_PATH = "C:/Users/natal/OneDrive/Escritorio/data_engineer_proyects/chatbot_ecommerce/results/checkpoint-21/"

# Cargar el tokenizer y el modelo fine-tuneado
# Cargar el tokenizer y el modelo fine-tuneado, ignorando los tamaños no coincidentes
tokenizer = GPT2Tokenizer.from_pretrained("gpt2-medium")
model = GPT2LMHeadModel.from_pretrained(MODEL_PATH, ignore_mismatched_sizes=True)

# Asegurarse de que el modelo esté en el dispositivo correcto
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

# Asegurar un token de padding para evitar errores
tokenizer.pad_token = tokenizer.eos_token

# Cargar y procesar CSV
def cargar_csv(ruta_csv):
    df = pd.read_csv(ruta_csv, dtype={
        'InvoiceNo': str,
        'StockCode': str,
        'Description': str,
        'Quantity': 'Int64',
        'InvoiceDate': str,
        'UnitPrice': float,
        'CustomerID': str,
        'Country': str,
        'Revenue': float,
        'Month': 'Int64'
    })
    print("CSV cargado exitosamente. Ejemplo de datos:")
    print(df.head())
    return df

# Fragmentar datos de texto para embeddings
def cargar_y_fragmentar_datos(ruta_csv):
    df = cargar_csv(ruta_csv)
    text_data = (df['Description'].fillna('') + ' in ' + df['Country'].fillna('')).tolist()
    fragmentos = [text for text in text_data if text]
    return fragmentos[:50]

# Procesar embeddings y almacenarlos en FAISS
def procesar_embeddings(fragmentos, embeddings):
    print("Procesando los embeddings por lotes...")
    
    test_embedding = np.array(embeddings.embed_query("test"))
    dimension_embedding = test_embedding.shape[0]
    
    index = faiss.IndexFlatL2(dimension_embedding)
    docstore = InMemoryDocstore({})
    index_to_docstore_id = {}

    for i, fragmento in enumerate(fragmentos):
        embedding = np.array(embeddings.embed_query(fragmento))
        index.add(np.array([embedding]))
        doc_id = len(index_to_docstore_id)
        index_to_docstore_id[doc_id] = doc_id
        docstore._dict[doc_id] = Document(page_content=fragmento)

    vectorstore = FAISS(embedding_function=embeddings, index=index, docstore=docstore, index_to_docstore_id=index_to_docstore_id)
    print("Embeddings procesados y almacenados en FAISS.")
    return vectorstore

# Configurar embeddings y procesar dataset
ruta_csv = "C:/Users/natal/OneDrive/Escritorio/data_engineer_proyects/ecommerce_data_cleaned.csv"
embeddings = OpenAIEmbeddings(model="text-embedding-ada-002", openai_api_key=openai_api_key)
fragmentos = cargar_y_fragmentar_datos(ruta_csv)
vectorstore = procesar_embeddings(fragmentos, embeddings)

# Generar respuestas con el modelo fine-tuneado
def generar_respuesta(contexto, pregunta):
    # Crear el prompt combinando contexto y pregunta
    prompt = f"Context: {contexto}\n\nQuestion: {pregunta}\nAnswer:"
    
    # Tokenizar el prompt
    inputs = tokenizer.encode(prompt, return_tensors="pt").to(device)
    
    # Generar la respuesta
    outputs = model.generate(inputs, max_length=150, num_return_sequences=1, temperature=0.7, top_p=0.9, pad_token_id=tokenizer.eos_token_id)
    
    # Decodificar la respuesta
    respuesta = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    # Extraer la respuesta después de "Answer:"
    respuesta = respuesta.split("Answer:")[-1].strip()
    return respuesta

# Función principal del chatbot
def chatbot_pregunta(pregunta):
    try:
        # Buscar contexto relevante usando FAISS
        docs = vectorstore.similarity_search(pregunta, k=1)
        contexto = docs[0].page_content if docs else "No se encontró contexto relevante."
        
        # Generar respuesta utilizando el contexto y la pregunta
        respuesta = generar_respuesta(contexto, pregunta)
        return respuesta
    except Exception as e:
        print(f"Error procesando la pregunta: {e}")
        return "Hubo un error al procesar la respuesta."

# Configuración de la interfaz de Gradio
descripcion = """
This chatbot is designed to answer questions about e-commerce data, including details on sales, customers, products, and more, using a fine-tuned model.
"""

gr.Interface(
    fn=chatbot_pregunta,
    inputs="text",
    outputs="text",
    title="E-commerce Chatbot with Fine-Tunning Model",
    description=descripcion
).launch(share=True,server_port=7860)

