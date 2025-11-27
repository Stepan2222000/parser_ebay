FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libbz2-dev \
    libreadline-dev \ 
    libsqlite3-dev \
    wget \
    llvm \
    libffi-dev \ 
    zlib1g-dev \
    liblzma-dev \
    xz-utils
RUN cd /root
RUN wget https://www.python.org/ftp/python/3.12.9/Python-3.12.9.tar.xz && \
    tar -xvf ./Python-3.12.9.tar.xz && \
    cd ./Python-3.12.9 && \
    ./configure --enable-optimizations && \
    make -j $(nproc) && \
    make install

WORKDIR /opt/app/
ADD pyproject.toml .
RUN python3 -m pip install uv
ENV UV_SYSTEM_PYTHON=1
RUN uv pip install -r pyproject.toml
RUN python3 -m pip install --no-cache-dir python-dotenv
RUN chmod +x /usr/local/lib/python3.12/site-packages/undetected_playwright/driver/playwright.sh
RUN chmod +x /usr/local/lib/python3.12/site-packages/undetected_playwright/driver/node
RUN python3 -m undetected_playwright install firefox
RUN python3 -m undetected_playwright install-deps
