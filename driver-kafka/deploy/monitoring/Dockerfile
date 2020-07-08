FROM grafana/grafana:7.0.5

USER root
COPY dashboards.yaml /etc/grafana/provisioning/dashboards/dashboards.yaml
RUN  apk add curl
USER grafana
COPY grafana.ini /etc/grafana/grafana.ini
COPY dashboards/* /var/lib/grafana/dashboards/
COPY start.sh /start.sh

EXPOSE 3000

ENV PROMETHEUS_URL http://prometheus:9090

ENTRYPOINT ["/start.sh"]
