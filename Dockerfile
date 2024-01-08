FROM python:3.10
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y && pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mv shell_tool/.lakectl.yaml ~
EXPOSE 8000
CMD [ "python", "./main.py" ]