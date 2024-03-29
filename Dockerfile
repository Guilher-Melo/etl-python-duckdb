# Use a imagem base do Python
FROM python:3.12

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia os arquivos necessários para o diretório de trabalho do contêiner
COPY requirements.txt .
COPY app.py .
COPY pipeline_02.py .
COPY /pasta_gdown .

# Instala as dependências especificadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

# Comando para executar o arquivo de pipeline
CMD ["streamlit", "run", "app.py", "--server.port", "8501"]
