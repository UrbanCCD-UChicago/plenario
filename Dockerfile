FROM python:2.7.12

# Get add-apt-repository
RUN apt-get update -y
RUN apt-get install -y software-properties-common

# Get GDAL
RUN add-apt-repository -y ppa:ubuntugis/ppa 
RUN apt-get update -y
RUN apt-get install gdal-bin

# Install required Python modules
RUN pip install -r requirements.txt

# Expose port 5000
EXPOSE 5000

# Vamonos!
CMD ["python", "runserver.py"]
