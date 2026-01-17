# SAP CDC Connector

This connector provides SAP Change Data Capture (CDC) integration for Nobu, supporting three connection methods:
- **JDBC** - Direct connection to SAP HANA database (currently disabled - see below)
- **REST API** - OData/REST API integration ✅ Available
- **SAP Data Services** - Integration with SAP Data Services replication server ✅ Available

## Current Status

**JDBC connector is currently disabled** because the SAP HANA JDBC driver (`ngdbc`) is not available in Maven Central. 
The JDBC implementation files are excluded from compilation. See "Enabling JDBC Support" below to enable it.

**REST and Data Services connectors are fully functional** and can be built and used immediately.

## Building

Build the connector (REST and Data Services connectors only):

```bash
mvn clean install -DskipTests
```

## Enabling JDBC Support

To enable the JDBC connector, you need to install the SAP HANA JDBC driver manually:

1. Download the SAP HANA JDBC driver from:
   - SAP Support Portal: https://support.sap.com/en/my-support/software-downloads.html
   - Or SAP Tools: https://tools.hana.ondemand.com/#hanatools
   - Look for "SAP HANA Client" or "SAP HANA JDBC Driver"

2. Extract the `ngdbc.jar` file from the downloaded package

3. Install to local Maven repository:
   ```bash
   mvn install:install-file \
     -Dfile=ngdbc.jar \
     -DgroupId=com.sap.cloud.db.jdbc \
     -DartifactId=ngdbc \
     -Dversion=2.19.24 \
     -Dpackaging=jar
   ```

4. Uncomment JDBC dependencies in `pom.xml`:
   - Uncomment the `<dependency>` blocks for `ngdbc` and `HikariCP`

5. Remove exclusions from `maven-compiler-plugin` in `pom.xml`:
   - Remove the `<excludes>` section that excludes `**/jdbc/*.java`

6. Uncomment JDBC connector configuration in `route.yaml`

See `src/main/java/com/nobu/connect/sap/jdbc/README_JDBC_DISABLED.txt` for more details.

## Configuration

See `route.yaml` for example configurations of all three connector types.

## Testing

Run unit tests:
```bash
mvn test -pl connectors/sap
```

Note: Integration tests may require actual SAP system access.
