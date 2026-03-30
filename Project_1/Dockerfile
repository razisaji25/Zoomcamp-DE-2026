FROM python:3.11

# Install Java (pakai default-jdk → lebih aman)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set JAVA_HOME otomatis
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]