import os

# Establece la ruta donde tienes tu kaggle.json
os.environ['KAGGLE_CONFIG_DIR'] = 'C:/Users/natal/OneDrive/Escritorio/data_engineer_proyects/kaggle.json'
# Para Linux o MacOS, sería algo como '/home/usuario/.kaggle/'

# Descargar el dataset desde Kaggle
dataset = 'carrie1/ecommerce-data'
os.system(f'kaggle datasets download -d {dataset} --unzip')
