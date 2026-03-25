FROM python:3.11

WORKDIR /app

COPY ./order_queue/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python -m grpc_tools.protoc -I utils/pb/order_queue --python_out=utils/pb/order_queue --pyi_out=utils/pb/order_queue --grpc_python_out=utils/pb/order_queue utils/pb/order_queue/order_queue.proto && python utils/other/hotreload.py "order_queue/src/app.py"
