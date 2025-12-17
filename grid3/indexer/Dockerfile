# Build this Dockerfile from the repo root:
# cd $(git rev-parse --show-toplevel)
# docker buildx build -t grid3-indexer -f grid3/indexer/Dockerfile .
FROM python:3.12-alpine

RUN mkdir /grid3 /tfchain-db
COPY . /grid3

RUN pip install /grid3[tfchain,indexer]

ENTRYPOINT ["python", "-m", "grid3.indexer"]
CMD ["--max-workers", "5", "-f", "/tfchain-db/tfchain.db"]
