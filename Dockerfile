FROM python:3.11

COPY WSManager/req.txt /WSManager/app/requirements.txt
WORKDIR /WSManager/app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY ./WSManager/ /WSManager/app

CMD ["python", "-u", "main.py"]

# Устанавливаем скрипт запуска как ENTRYPOINT
#ENTRYPOINT ["/WSManager/app/main.py"]

