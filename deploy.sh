#,chunjun-connectors/chunjun-connector-elasticsearch7
 mvn clean install -pl chunjun-connectors/chunjun-connector-mysql\
,chunjun-connectors/chunjun-connector-clickhouse\
,chunjun-connectors/chunjun-connector-doris\
,chunjun-connectors/chunjun-connector-postgresql\
,chunjun-connectors/chunjun-connector-greenplum\
,chunjun-connectors/chunjun-connector-oracle\
,chunjun-connectors/chunjun-connector-starrocks\
 -am \
 -Dmaven.test.skip=true -Dspotless.check.skip=true -DaltDeploymentRepository=base::default::http://localhost:8080/release
