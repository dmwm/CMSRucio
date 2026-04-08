FROM registry.cern.ch/cmsrucio/file_invalidation_tool:base
WORKDIR /app
COPY ./src/ .
ENTRYPOINT ["python3", "run_check.py"]
