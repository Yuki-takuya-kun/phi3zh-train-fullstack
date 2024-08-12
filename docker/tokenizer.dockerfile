ARG tokenizerDir
ARG expose

FROM python:3.10
WORKDIR /usr/src/app

RUN pip install transformers

MKDIR tokenizer

COPY ${tokenizerDir}$ ./tokenizer

COPY ../src/main/python/service/tokenizer_service.py /usr/src/app

EXPOSE ${expose}$

CMD ["python", "./tokenizer_service.py", "port=${expose}$", "mode_dir=./tokenizer"]