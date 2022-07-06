FROM pytorch/pytorch

COPY requirements.txt /project/
RUN pip install -r /project/requirements.txt

# Install project
COPY lib /project/lib
COPY big_query_to_bucket_dag.py /project/
COPY proven-mind-352922-08414cb4ab55.json /project/
COPY test.py /project/

ENTRYPOINT ["python", "/project/test.py"]
