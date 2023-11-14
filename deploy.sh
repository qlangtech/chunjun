#,chunjun-connectors/chunjun-connector-elasticsearch7
 mvn clean install -fn -pl chunjun-connectors/chunjun-connector-mysql\
,chunjun-connectors/chunjun-connector-clickhouse\
,chunjun-connectors/chunjun-connector-doris\
,chunjun-connectors/chunjun-connector-postgresql\
,chunjun-connectors/chunjun-connector-dameng\
,chunjun-connectors/chunjun-connector-oracle\
,chunjun-connectors/chunjun-connector-starrocks\
,chunjun-connectors/chunjun-connector-kafka\
,chunjun-connectors/chunjun-connector-rabbitmq\
 -am \
 -Dmaven.test.skip=true -Dspotless.check.skip=true -Dtis-repo
