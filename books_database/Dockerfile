FROM python:3.11

WORKDIR /app

COPY ./books_database/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python -m grpc_tools.protoc -I utils/pb/books_database --python_out=utils/pb/books_database --pyi_out=utils/pb/books_database --grpc_python_out=utils/pb/books_database utils/pb/books_database/books_database.proto && python utils/other/hotreload.py "books_database/src/app.py"
