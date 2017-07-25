# DBC repository

# ojdbc8 driver
## use oracle maven
```
<groupid>com.oracle.jdbc</groupid>
<artifactid>ojdbc8</artifactid>
<version>12.2.0.1</version>
```
https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides

settings-security.xml
```
<settingssecurity>
<master>{By8wW7YcTxAHof0MF4Z3wPKboywhGJvxHD9m0NvHA2U=}></master>
</settingssecurity>
```

settings.xml
```
<settings>
  <proxies>
    <proxy>
      <active>true</active>
      <protocol>http</protocol>
      <host>proxy.mycompany.com</host>
      <nonProxyHosts>mycompany.com</nonProxyHosts>
   </proxy>
  </proxies>

<servers>
  <server>
    <id>maven.oracle.com </id>
    <username>firstname.lastname@test.com</username>
    <password>{pnwmhVnzdM8H3UAneUKLmaHGZCoaprbMQ/Ac5UktvsM=}</password>
  <configuration>
    <basicAuthScope>
      <host>ANY </host>
      <port>ANY </port>
      <realm>OAM 11g </realm>
    </basicAuthScope>
    <httpConfiguration>
      <all>
      <params>
        <property>
          <name>http.protocol.allow-circular-redirects </name>
          <value>%b,true </value>
        </property>
      </params>
      </all>
    </httpConfiguration>
  </configuration>
  </server>
  </servers>
</settings>
```
## import in m2
```
mvn install:install-file -Dfile={Path/to/your/ojdbc7.jar}
      -DgroupId=com.oracle.jdbc -DartifactId=ojdbc7 -Dversion=12.1.0 -Dpackaging=jar


mvn install:install-file -Dfile={Path/to/your/ojdbc8.jar}
      -DgroupId=com.oracle.jdbc -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar

```

# Install DB with docker

https://github.com/oracle/docker-images/tree/master/OracleDatabase

```
./buildDockerImage.sh -v 12.2.0.1 -e -o 12.2.0.1 -o --squash

docker run -d --name db12c -p 1521:1521 -p 5500:5500 -e ORACLE_PWD=welcome1 -e ORACLE_SID=ORCL -v /Users/matthiasfuchs/projects/dbc/docker:/opt/oracle/oradata oracle/database:12.2.0.1-ee

```

# Change DB

```
CREATE TEMPORARY TABLESPACE temp2
   TEMPFILE 'temp01.dbf' SIZE 500M AUTOEXTEND ON;

ALTER DATABASE DEFAULT TEMPORARY TABLESPACE temp2;
```

# use swingbench to make some load

"137";"19";"0";"19";"52398";"30030";"SOE";"AG-001117.fritz.box";"unknown";"matthiasfuchs";"JDBC Thin Client";"WAITING";"";"";"";"147a57cxq3w5y";"0";"2017-07-25 20:42:47";"";"";"";"Swingbench Load Generator";"orclpdb1";"";"ACTIVE";"";"36012";
....
