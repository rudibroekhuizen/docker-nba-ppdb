FROM python:3.6.5-stretch

RUN apt-get update \
  && apt-get install -y \
    python3-postgresql \
    libpq-dev \
    libfreetype6-dev \
    libpng-dev \
    libqhull-dev \
    bash \
    git

RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt .
RUN  pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt \
    && jupyter contrib nbextension install --system \
    && mkdir -p $(jupyter --data-dir)/nbextensions \
    && cd $(jupyter --data-dir)/nbextensions  \
    && git clone https://github.com/lambdalisue/jupyter-vim-binding vim_binding \
    && jupyter nbextension enable vim_binding/vim_binding

EXPOSE 8000

ENV PYTHONUNBUFFERED 1

COPY . .
