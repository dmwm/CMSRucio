FROM registry.cern.ch/cmsrucio/file_invalidation_tool:base

# Expose ports
EXPOSE 5001
EXPOSE 5002
EXPOSE 5003

# Copy code
COPY ./src /src/

# Set working directory
WORKDIR /src

RUN chmod 755 /src/submit_invalidation.sh

# Set entrypoint
ENTRYPOINT ["python3", "run_invalidations.py"]