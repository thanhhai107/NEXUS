FROM bitnamilegacy/spark:3.5

USER root
RUN if ! getent passwd 1001 >/dev/null; then \
      echo "spark:x:1001:0:Spark:/tmp/nexus-spark-home:/bin/bash" >> /etc/passwd; \
    fi
USER 1001
