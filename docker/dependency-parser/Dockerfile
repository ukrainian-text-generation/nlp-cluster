FROM python:3.10 AS base
WORKDIR /app
RUN pip install -U pip setuptools wheel spacy
RUN python -m spacy download uk_core_news_lg
COPY src/dependency-parser/main.py src/main.py
EXPOSE 8000
RUN useradd user
USER user
CMD ["python", "src/main.py"]