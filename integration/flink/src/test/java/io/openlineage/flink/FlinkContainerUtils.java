package io.openlineage.flink;

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class FlinkContainerUtils {

  private static final String NETWORK_ALIAS = "openlineageflink";

  static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(
            DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.12.0"))
        .withNetwork(network)
        .withNetworkAliases(NETWORK_ALIAS);
  }

  static GenericContainer<?> makeSchemaRegistryContainer(Network network, Startable startable) {
    return genericContainer(network, "confluentinc/cp-schema-registry:6.2.1", "schema-registry")
        .withExposedPorts(28081)
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://schema-registry:8081,http://0.0.0.0:28081")
        .dependsOn(startable);
  }

  static GenericContainer<?> makeKafkaContainer(Network network, Startable zookeeper) {
    return genericContainer(network, "confluentinc/cp-kafka:6.2.1", "kafka")
        .withExposedPorts(9092, 19092)
        .withEnv(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "LISTENER_DOCKER_INTERNAL:PLAINTEXT,LISTENER_DOCKER_EXTERNAL:PLAINTEXT")
        .withEnv(
            "KAFKA_ADVERTISED_LISTENERS",
            "LISTENER_DOCKER_INTERNAL://kafka:9092,LISTENER_DOCKER_EXTERNAL://127.0.0.1:19092")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "LISTENER_DOCKER_INTERNAL")
        .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .withEnv("KAFKA_BROKER_ID", "1")
        .withEnv(
            "KAFKA_LOG4J_LOGGERS",
            "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO")
        .dependsOn(zookeeper);
  }

  @SneakyThrows
  static GenericContainer<?> makeInitKafkaContainer(Network network, Startable kafka) {
    return genericContainer(network, "confluentinc/cp-kafka:6.2.1", "init-kafka")
        .withCreateContainerCmdModifier(cmd -> cmd.withHostName("init-kafka"))
        .withCommand(
            "/bin/sh",
            "-c",
            Resources.toString(Resources.getResource("kafka_init.sh"), StandardCharsets.UTF_8))
        .dependsOn(kafka);
  }

  @SneakyThrows
  static GenericContainer<?> makeGenerateEventsContainer(Network network, Startable initTopics) {
    return genericContainer(network, "confluentinc/cp-schema-registry:6.2.1", "generate-events")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("InputEvent.avsc").getPath()),
            "/tmp/InputEvent.avsc")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("events.json").getPath()),
            "/tmp/events.json")
        .withCommand(
            "/bin/sh",
            "-c",
            Resources.toString(Resources.getResource("generate_events.sh"), StandardCharsets.UTF_8))
        .dependsOn(initTopics);
  }

  static GenericContainer<?> makeZookeeperContainer(Network network) {
    return genericContainer(network, "confluentinc/cp-zookeeper:6.2.1", "zookeeper")
        .withExposedPorts(2181)
        .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
        .withEnv("ZOOKEEPER_SERVER_ID", "1");
  }

  static GenericContainer<?> makeFlinkJobManagerContainer(Network network, Startable startable) {
    GenericContainer<?> container =
        genericContainer(network, "flink:1.14.2-scala_2.11-java11", "jobmanager")
            .withExposedPorts(8081)
            .withCopyFileToContainer(
                MountableFile.forHostPath(getOpenLineageJarPath()),
                "/opt/flink/lib/openlineage.jar")
            .withCopyFileToContainer(
                MountableFile.forHostPath(getExampleAppJarPath()), "/opt/flink/lib/example-app.jar")
            // .waitingFor(Wait.forLogMessage("Openlineage event emitted", 1))
            .withCommand(
                Stream.of(
                        new String[] {
                          "standalone-job",
                          "--job-classname",
                          "io.openlineage.flink.FlinkStatefulApplication",
                          "--input-topic",
                          "io.openlineage.flink.kafka.input",
                          "--output-topic",
                          "io.openlineage.flink.kafka.output"
                        })
                    .toArray(String[]::new))
            .withEnv("FLINK_PROPERTIES", "jobmanager.rpc.address: jobmanager")
            .dependsOn(startable);

    return container;
  }

  static GenericContainer<?> makeFlinkTaskManagerContainer(Network network, Startable startable) {
    return genericContainer(network, "flink:1.14.2-scala_2.11-java11", "taskmanager")
        .withCopyFileToContainer(
            MountableFile.forHostPath(getOpenLineageJarPath()), "/opt/flink/lib/openlineage.jar")
        .withCopyFileToContainer(
            MountableFile.forHostPath(getExampleAppJarPath()), "/opt/flink/lib/example-app.jar")
        .withEnv("FLINK_PROPERTIES", "jobmanager.rpc.address: jobmanager")
        .withCommand("taskmanager")
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .dependsOn(startable);
  }

  private static GenericContainer<?> genericContainer(
      Network network, String image, String hostname) {
    return new GenericContainer<>(DockerImageName.parse(image))
        .withNetwork(network)
        .withNetworkAliases(NETWORK_ALIAS)
        .withLogConsumer(
            new Consumer<OutputFrame>() {
              @Override
              public void accept(OutputFrame of) {
                try {
                  switch (of.getType()) {
                    case STDOUT:
                      System.out.write(
                          of.getUtf8String()
                              .replace(
                                  System.lineSeparator(),
                                  System.lineSeparator() + "[" + hostname + "]")
                              .getBytes());
                      break;
                    case STDERR:
                      System.err.write(
                          of.getUtf8String()
                              .replace(
                                  System.lineSeparator(),
                                  System.lineSeparator() + "[" + hostname + "]")
                              .getBytes());
                      break;
                    case END:
                      System.out.println(of.getUtf8String());
                      break;
                  }
                } catch (IOException ioe) {
                  throw new RuntimeException(ioe);
                }
              }
            })
        .withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname))
        .withReuse(true);
  }

  private static String getOpenLineageJarPath() {
    return Arrays.stream((new File("build/libs")).listFiles())
        .filter(file -> file.getName().startsWith("openlineage-flink"))
        .map(file -> file.getPath())
        .findAny()
        .get();
  }

  private static String getExampleAppJarPath() {
    return Arrays.stream((new File("examples/stateful/build/libs")).listFiles())
        .filter(file -> file.getName().startsWith("stateful"))
        .map(file -> file.getPath())
        .findAny()
        .get();
  }
}
