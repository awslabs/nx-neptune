# Athena Connectors

This directory contains Athena federated query connectors and their shared dependencies.

## Structure

```
connectors/
├── pom.xml                              # Parent POM (multi-module build)
├── aws-athena-query-federation/         # Git submodule (pinned to specific version)
│   └── athena-jdbc/                     # JDBC base module used by Databricks connector
├── athena-databricks-connector/         # Databricks Unity Catalog connector
└── athena-s3vector-connector/           # S3 Vector connector
```

## Why the Submodule?

The `athena-databricks-connector` depends on the `athena-jdbc` module from the [AWS Athena Query Federation SDK](https://github.com/awslabs/aws-athena-query-federation). This module provides base classes for JDBC-based connectors (`JdbcMetadataHandler`, `JdbcRecordHandler`, connection management, etc.).

The `athena-jdbc` module is **not published to Maven Central**, so it cannot be pulled as a regular Maven dependency. The submodule allows us to build it from source as part of the multi-module Maven build, without copying source files into our repository.

## Build

From the repository root:

```bash
# Initialize submodule (first time or after clone)
git submodule update --init

# Build all modules
mvn -f connectors/pom.xml clean package -DskipTests

# Build only the Databricks connector (uses cached athena-jdbc from .m2)
mvn -f connectors/pom.xml package -DskipTests -pl :athena-databricks-connector

# Build Databricks connector + its dependencies
mvn -f connectors/pom.xml package -DskipTests -pl :athena-databricks-connector -am
```

## Updating the Federation SDK Version

1. Check available tags:
   ```bash
   cd connectors/aws-athena-query-federation
   git fetch --tags
   git tag | grep v2026
   ```

2. Checkout the desired version:
   ```bash
   git checkout v2026.12.0
   cd ../..
   ```

3. Update the `athena-sdk.version` property in `athena-databricks-connector/pom.xml` to match.

4. Commit the submodule update:
   ```bash
   git add connectors/aws-athena-query-federation
   git commit -m "Bump federation-sdk to v2026.12.0"
   ```

5. Rebuild to verify:
   ```bash
   mvn -f connectors/pom.xml clean package -DskipTests
   ```
