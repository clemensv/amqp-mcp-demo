FROM icr.io/ibm-messaging/mq:10.0.0.0-r1 AS amqp-package
USER 0
COPY --from=busybox:1.37.0-glibc /bin/busybox /usr/local/bin/gzip
ADD https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqadv/10.0.0.0-IBM-MQ-Advanced-for-Developers-Non-Install-LinuxX64.tar.gz /tmp/mq.tar.gz
RUN mkdir /opt/mqm-full && tar -xzf /tmp/mq.tar.gz -C /opt/mqm-full
ENV genmqpkg_inc32=0 genmqpkg_incadm=0 genmqpkg_incamqp=1 \
    genmqpkg_incams=0 genmqpkg_inccbl=0 genmqpkg_inccics=0 \
    genmqpkg_inccpp=0 genmqpkg_incdnet=0 genmqpkg_incjava=0 \
    genmqpkg_incjre=0 genmqpkg_incman=0 genmqpkg_incmqbc=0 \
    genmqpkg_incmqft=0 genmqpkg_incmqsf=0 genmqpkg_incmqxr=0 \
    genmqpkg_incnls=0 genmqpkg_incras=0 genmqpkg_incsamp=0 \
    genmqpkg_incsdk=0 genmqpkg_inctls=0 genmqpkg_incnothrd=0 \
    genmqpkg_incweb=0
RUN /opt/mqm-full/bin/genmqpkg.sh -b /opt/mqm-amqp

FROM icr.io/ibm-messaging/mq:10.0.0.0-r1
USER 0
COPY --chown=1001:root --from=amqp-package /opt/mqm-amqp/amqp/ /opt/mqm/amqp/
COPY --chown=1001:root --from=amqp-package /opt/mqm-amqp/bin/amqp.sh /opt/mqm/bin/amqp.sh
USER 1001