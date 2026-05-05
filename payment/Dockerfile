FROM python:3.11

WORKDIR /app

COPY ./payment/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python -m grpc_tools.protoc -I utils/pb/payment --python_out=utils/pb/payment --pyi_out=utils/pb/payment --grpc_python_out=utils/pb/payment utils/pb/payment/payment.proto && python utils/other/hotreload.py "payment/src/app.py"
