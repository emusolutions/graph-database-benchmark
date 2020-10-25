FROM python:2.7.10

COPY scripts ./scripts
COPY bulk_insert.py ./
COPY config.py ./
COPY generate_graph500_inputs.py ./
COPY generate_twitter_inputs.py ./
COPY graph_query.py ./
COPY kn.py ./
COPY query_runner.py ./
COPY requirements.txt ./
COPY docker_entrypoint.sh ./

#RUN ./scripts/retrieve_seed_and_unique_node_data_from_s3.sh

RUN python -m pip install -r requirements.txt


RUN chmod -R 751 scripts
RUN chmod 751 docker_entrypoint.sh
ENTRYPOINT ["./docker_entrypoint.sh"]