package io.openlineage.flink;

import static org.mockserver.model.HttpRequest.request;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Testcontainers
@Slf4j
public class OpenLineageFlinkContainerTest {

  private static final Network network = Network.newNetwork();
  private static MockServerClient mockServerClient;

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      FlinkContainerUtils.makeMockServerContainer(network);

  @Container
  private static final GenericContainer zookeeper =
      FlinkContainerUtils.makeZookeeperContainer(network);

  @Container
  private static final GenericContainer kafka =
      FlinkContainerUtils.makeKafkaContainer(network, zookeeper);

  @Container
  private static final GenericContainer schemaRegistry =
      FlinkContainerUtils.makeSchemaRegistryContainer(network, kafka);

  @Container
  private static final GenericContainer initKafka =
      FlinkContainerUtils.makeInitKafkaContainer(network, schemaRegistry);

  @Container
  private static final GenericContainer generateEvents =
      FlinkContainerUtils.makeGenerateEventsContainer(network, initKafka);

  @Container
  private static final GenericContainer jobManager =
      FlinkContainerUtils.makeFlinkJobManagerContainer(network, generateEvents);

  @Container
  private static final GenericContainer taskManager =
      FlinkContainerUtils.makeFlinkTaskManagerContainer(network, jobManager);

  @BeforeAll
  public static void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
      zookeeper.stop();
      schemaRegistry.stop();
      kafka.stop();
      jobManager.stop();
      taskManager.stop();
      initKafka.stop();
    } catch (Exception e) {
      log.error("Unable to shut down containers", e);
    }
    network.close();
  }

  @Test
  public void testOpenLineageEventSent() {
    taskManager.start();
  }
}
