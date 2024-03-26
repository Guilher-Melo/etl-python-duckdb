import streamlit as st

from pipeline_02 import pipeline

st.title('Processador de arquivos')

if st.button('Processar'):
    with st.spinner('Processando...'):
        logs = pipeline()
        # Exibe os logs
        for log in logs:
            st.write(log)
