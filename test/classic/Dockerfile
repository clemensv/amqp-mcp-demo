FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location https://archive.apache.org/dist/activemq/6.3.2/apache-activemq-6.3.2-bin.tar.gz -o /tmp/activemq.tar.gz \
    && mkdir /opt/activemq && tar -xzf /tmp/activemq.tar.gz -C /opt/activemq --strip-components=1 \
    && rm /tmp/activemq.tar.gz && useradd --system --home /opt/activemq activemq \
    && chown -R activemq:activemq /opt/activemq
COPY --chown=activemq:activemq activemq.xml /opt/activemq/conf/activemq.xml
USER activemq
WORKDIR /opt/activemq
EXPOSE 5672
CMD ["sh", "-c", "umask 077; printf 'activemq.username=admin\nactivemq.password=%s\n' \"$AMQP_DEMO_CLASSIC_PASSWORD\" > conf/credentials.properties; exec bin/activemq console"]