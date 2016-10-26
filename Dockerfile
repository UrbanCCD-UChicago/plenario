FROM jesseb954/plenario

EXPOSE 5000
WORKDIR "/src"
ADD . .
RUN pip install -r requirements.txt
CMD python runserver.py
