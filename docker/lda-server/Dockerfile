FROM python:3.10 AS base
WORKDIR /app
RUN pip install -U pip setuptools wheel spacy gensim scipy==1.10.1
RUN python -m spacy download uk_core_news_lg
COPY src/lda-server/main.py src/main.py
COPY data/lda/model data/

RUN ls -l data/

RUN for file in data/*.model*; do \
      base=$(basename "$file"); \
      newname="lda.${base#*.}"; \
      mv "data/$base" "data/$newname"; \
    done

RUN ls -l data/

EXPOSE 8000
RUN useradd user
USER user
CMD ["python", "src/main.py"]