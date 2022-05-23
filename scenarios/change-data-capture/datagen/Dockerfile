FROM python:3.9

RUN pip install --no-cache-dir mysql-connector-python pravega===0.3.1 yfinance

COPY datagen.py ./

CMD [ "python", "-u", "./datagen.py" ]
