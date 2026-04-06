FROM python:3.11

WORKDIR /app

COPY ./executor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python -m grpc_tools.protoc -I utils/pb/executor --python_out=utils/pb/executor --pyi_out=utils/pb/executor --grpc_python_out=utils/pb/executor utils/pb/executor/executor.proto && python -m grpc_tools.protoc -I utils/pb/order_queue --python_out=utils/pb/order_queue --pyi_out=utils/pb/order_queue --grpc_python_out=utils/pb/order_queue utils/pb/order_queue/order_queue.proto && python utils/other/hotreload.py "executor/src/app.py"
