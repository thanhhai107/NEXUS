FROM apache/hive:4.0.0

USER root
ADD --chmod=0644 --chown=hive:hive https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgresql.jar
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar /opt/hive/lib/ \
    && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar /opt/hive/lib/ \
    && chown hive:hive /opt/hive/lib/hadoop-aws-*.jar /opt/hive/lib/aws-java-sdk-bundle-*.jar
COPY --chown=hive:hive core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY --chown=hive:hive core-site.xml /opt/hive/conf/core-site.xml
USER hive
