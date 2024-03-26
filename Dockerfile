# Use a imagem base do Python
FROM python:3.12

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia os arquivos necessários para o diretório de trabalho do contêiner
COPY requirements.txt .
COPY app.py .
COPY pipeline_02.py .

# Instala as dependências especificadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Comando para executar o arquivo de pipeline
CMD ["python", "app.py"]
