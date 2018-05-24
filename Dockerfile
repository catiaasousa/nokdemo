FROM python:3.6
LABEL maintainer "luisfernando.filipe@.com"
ENV ST2_ADDRESS stackstorm
ENV API_KEY ZTNlNjM0NzY1ZDE0ZDEyZjNmNTc3MzNiMjUwOGI4MWQ1MGFmM2RlMGNjNmIwMGM3Yzg4MDllYmMxZTI4NjNhOA
RUN apt-get update
COPY  nokdemo /nokdemo/nokdemo/
COPY setup.py MANIFEST.in /nokdemo/
WORKDIR /nokdemo
EXPOSE 5000
RUN python setup.py install
RUN pip install -r nokdemo/requirements.txt
ENTRYPOINT ["python", "nokdemo/app.py"]