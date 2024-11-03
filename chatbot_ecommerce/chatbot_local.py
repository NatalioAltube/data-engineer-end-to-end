from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores.faiss import FAISS
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain.schema import Document
import openai
import faiss
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os


# Cargar variables desde el archivo .env
load_dotenv()

# Ahora puedes acceder a la clave desde el entorno
openai.api_key = os.getenv("OPENAI_API_KEY")

# Inicializa la aplicación de FastAPI
app = FastAPI()

# Clase para el formato de preguntas
class Question(BaseModel):
    pregunta: str

# Endpoint para verificar si el servidor está funcionando
@app.get("/")
async def root():
    return {"message": "Servidor de chatbot E-commerce está activo"}

# Cargar y procesar embeddings
def configurar_openai_embeddings():
    try:
        print("Cargando modelo de embeddings de OpenAI...")
        embeddings = OpenAIEmbeddings(model="text-embedding-ada-002", openai_api_key=openai.api_key)
        print("Modelo de embeddings cargado exitosamente.")
        return embeddings
    except Exception as e:
        print(f"Error al configurar OpenAIEmbeddings: {e}")
        raise

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

# Cargar y fragmentar datos para embeddings
def cargar_y_fragmentar_datos(ruta_csv):
    df = cargar_csv(ruta_csv)
    text_data = (df['Description'].fillna('') + ' in ' + df['Country'].fillna('')).tolist()
    fragmentos = [text for text in text_data if text]
    return fragmentos[:50]  # Limitar a 50 fragmentos para pruebas

# Procesar embeddings usando FAISS
def procesar_embeddings(fragmentos, embeddings):
    print("Procesando los embeddings por lotes...")
    
    # Asegúrate de que los embeddings generados sean un array de NumPy
    test_embedding = np.array(embeddings.embed_query("test"))
    dimension_embedding = test_embedding.shape[0]  # Obtener la dimensión de un embedding de prueba
    
    index = faiss.IndexFlatL2(dimension_embedding)
    docstore = InMemoryDocstore({})
    index_to_docstore_id = {}

    for i, fragmento in enumerate(fragmentos):
        embedding = np.array(embeddings.embed_query(fragmento))  # Convertir a array NumPy aquí
        index.add(np.array([embedding]))  # Agregar embedding al índice
        doc_id = len(index_to_docstore_id)
        index_to_docstore_id[doc_id] = doc_id
        docstore._dict[doc_id] = Document(page_content=fragmento)

    vectorstore = FAISS(embedding_function=embeddings, index=index, docstore=docstore, index_to_docstore_id=index_to_docstore_id)
    print("Embeddings procesados y almacenados en FAISS.")
    return vectorstore

# Configurar los embeddings y procesar el dataset
ruta_csv = "C:/Users/natal/OneDrive/Escritorio/data_engineer_proyects/ecommerce_data_cleaned.csv"
embeddings = configurar_openai_embeddings()
fragmentos = cargar_y_fragmentar_datos(ruta_csv)
vectorstore = procesar_embeddings(fragmentos, embeddings)

# Función para generar respuestas con OpenAI GPT-4
def generar_respuesta(contexto, pregunta):
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": f"Context: {contexto}\n\nQuestion: {pregunta}"}
    ]
    try:
        respuesta = openai.ChatCompletion.create(
            model="gpt-4",
            messages=messages,
            max_tokens=150,
            temperature=0
        )
        return respuesta.choices[0].message['content'].strip()
    except Exception as e:
        print(f"Error en la generación de respuesta: {e}")
        return "Error en la generación de respuesta."

# Endpoint para recibir preguntas y dar respuestas
@app.post("/preguntar")
async def preguntar(question: Question):
    try:
        print(f"Pregunta recibida: {question.pregunta}")
        docs = vectorstore.similarity_search(question.pregunta, k=1)
        if not docs or not docs[0]:
            raise ValueError("No se encontraron documentos relevantes.")
        contexto = docs[0].page_content
        respuesta = generar_respuesta(contexto, question.pregunta)
        return {"respuesta": respuesta}
    except Exception as e:
        print(f"Error procesando la pregunta: {e}")
        raise HTTPException(status_code=500, detail=f"Error procesando la pregunta: {e}")

# Ejecutar el servidor Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)