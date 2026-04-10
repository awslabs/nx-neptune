ARG JAVA_VERSION=11
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

ARG JAR_VERSION=0.1.0
COPY target/athena-databricks-connector-${JAR_VERSION}.jar ${LAMBDA_TASK_ROOT}
RUN jar xf ${LAMBDA_TASK_ROOT}/athena-databricks-connector-${JAR_VERSION}.jar

CMD ["com.amazonaws.athena.connectors.databricks.DatabricksCompositeHandler"]
