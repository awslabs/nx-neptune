ARG JAVA_VERSION=11
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

COPY target/athena-databricks-connector-0.1.0.jar ${LAMBDA_TASK_ROOT}
RUN jar xf athena-databricks-connector-0.1.0.jar

CMD ["com.amazonaws.athena.connectors.databricks.DatabricksCompositeHandler"]
