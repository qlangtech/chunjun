 mvn deploy -pl chunjun-connectors/chunjun-connector-mysql\
,chunjun-connectors/chunjun-connector-clickhouse\
,chunjun-connectors/chunjun-connector-elasticsearch7 -am \
 -Dmaven.test.skip=true -DaltDeploymentRepository=base::default::http://localhost:8080/release
