FROM python:3.10 AS base
WORKDIR /app
COPY src/corrector/main.py src/main.py
EXPOSE 8000
RUN useradd user
USER user
CMD ["python", "src/main.py"]