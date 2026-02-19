# Part 8: Deployment

This guide explains how to package your Wabase application as a standalone "fat JAR" using `sbt-assembly`.

## 1. Configure sbt-assembly

Ensure `project/plugins.sbt` contains:
```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
```

In `build.sbt`, add the `assemblySettings` to handle merge conflicts (common with Pekko and logging libraries).

```scala
// Merge strategy for fat jar
assembly / assemblyMergeStrategy := {
  // Discard module-info.class (Java 9+ modules)
  case PathList("META-INF", "versions", rest @ _*)
    if rest.lastOption.contains("module-info.class") => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard

  // Concatenate reference.conf files (important for Akka/Pekko)
  case "reference.conf" => MergeStrategy.concat

  // Handle Service Provider Interface (SPI) files
  case PathList("META-INF", "services", _*) => MergeStrategy.concat

  // Discard license files to avoid conflicts
  case "LICENSE" | "LICENSE.txt" | "NOTICE" | "NOTICE.txt" => MergeStrategy.discard

  // Handle Jakarta Activation/Mail conflicts (if using email features)
  case PathList("jakarta", "activation", _*) => MergeStrategy.first
  case PathList("jakarta", "mail", _*) => MergeStrategy.first

  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
```

## 2. Build the JAR

Run the following command:

```bash
sbt assembly
```

This will create a JAR file in `target/scala-2.13/wabase-tms-assembly-0.1.0.jar`.

## 3. Run in Production

You can run the JAR on any machine with Java installed. You typically want to provide a production configuration file.

Create `prod.conf`:
```hocon
include "application"

jdbc.cp.main {
  jdbcUrl = "jdbc:postgresql://prod-db:5432/tms_prod"
  username = ${DB_USER}
  password = ${DB_PASS}
}

app.host = "https://tms.example.com"
```

Run the application:

```bash
java -Dconfig.file=prod.conf -jar target/scala-2.13/wabase-tms-assembly-0.1.0.jar
```

## Docker Deployment

You can wrap this JAR in a Docker container.

`Dockerfile`:
```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/scala-2.13/wabase-tms-assembly-0.1.0.jar app.jar
CMD ["java", "-jar", "app.jar"]
```

Build and run:
```bash
docker build -t wabase-tms .
docker run -p 8080:8080 -e DB_USER=admin -e DB_PASS=secret wabase-tms
```

**Next Step:** [Additional Features](09-additional-features.md)
