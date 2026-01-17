JDBC Implementation Temporarily Disabled
=========================================

The JDBC implementation classes in this directory are currently excluded from compilation
because the SAP HANA JDBC driver (ngdbc) is not available in Maven Central and must be
installed manually.

To enable JDBC support:

1. Download SAP HANA JDBC driver from:
   https://tools.hana.ondemand.com/#hanatools
   or
   https://support.sap.com/en/my-support/software-downloads.html

2. Install to local Maven repository:
   mvn install:install-file \
     -Dfile=ngdbc.jar \
     -DgroupId=com.sap.cloud.db.jdbc \
     -DartifactId=ngdbc \
     -Dversion=2.19.24 \
     -Dpackaging=jar

3. Uncomment JDBC dependencies in pom.xml:
   - Uncomment the ngdbc and HikariCP dependencies

4. Remove exclusions from maven-compiler-plugin in pom.xml:
   - Remove the <excludes> section for jdbc/*.java

5. Uncomment JDBC connector configuration in route.yaml

Files in this directory:
- JdbcProtocolHandler.java - JDBC protocol implementation
- JdbcSapCdcConnector.java - JDBC connector implementation
