# Build this Dockerfile from the repo root:
# cd $(git rev-parse --show-toplevel)
# docker buildx build -t grid3-archiver -f grid3/tfchain/archiver/Dockerfile .
FROM python:3.12-alpine

RUN mkdir /grid3 /tfchain-archive
COPY . /grid3

RUN pip install /grid3[tfchain,archiver]

ENTRYPOINT ["python", "-m", "grid3.tfchain.archiver"]
CMD ["--max-workers", "5", "-f", "/tfchain-archive/tfchain_archive.db"]
