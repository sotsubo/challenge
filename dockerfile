From python:latest
WORKDIR /challenge

RUN pip install --no-cache-dir fastapi uvicorn requests jsonify pandas sqlalchemy mysql-connector-python 

COPY . /challenge
EXPOSE 8000
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]