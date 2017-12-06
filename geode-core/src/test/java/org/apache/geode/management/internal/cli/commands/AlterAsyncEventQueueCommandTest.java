/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.AlterAsyncEventQueueCommand.BATCH_SIZE;
import static org.apache.geode.management.internal.cli.commands.AlterAsyncEventQueueCommand.BATCH_TIME_INTERVAL;
import static org.apache.geode.management.internal.cli.commands.AlterAsyncEventQueueCommand.ID;
import static org.apache.geode.management.internal.cli.commands.AlterAsyncEventQueueCommand.MAXIMUM_QUEUE_MEMORY;
import static org.apache.geode.management.internal.cli.commands.AlterAsyncEventQueueCommand.MAX_QUEUE_MEMORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class AlterAsyncEventQueueCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private AlterAsyncEventQueueCommand command;
  private ClusterConfigurationService service;
  private Region<String, Configuration> configRegion;

  @Before
  public void before() throws Exception {
    command = spy(AlterAsyncEventQueueCommand.class);
    service = mock(ClusterConfigurationService.class);
    doReturn(service).when(command).getSharedConfiguration();
    configRegion = mock(Region.class);
    when(service.getConfigurationRegion()).thenReturn(configRegion);
    when(service.lockSharedConfiguration()).thenReturn(true);

    when(configRegion.keySet())
        .thenReturn(Arrays.stream("group1,group2".split(",")).collect(Collectors.toSet()));
    Configuration configuration1 = new Configuration("group1");
    configuration1.setCacheXmlContent(getCacheXml("queue1"));
    when(configRegion.get("group1")).thenReturn(configuration1);

    Configuration configuration2 = new Configuration("group2");
    configuration2.setCacheXmlContent(getCacheXml("queue2"));
    when(configRegion.get("group2")).thenReturn(configuration2);

  }

  @Test
  public void mandatoryOption() throws Exception {
    gfsh.executeAndAssertThat(command, "alter async-event-queue").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void noOptionToModify() throws Exception {
    gfsh.executeAndAssertThat(command, "alter async-event-queue --id=test").statusIsError()
        .containsOutput("need to specify at least one option to modify.");
  }

  @Test
  public void emptyConfiguration() throws Exception {
    gfsh.executeAndAssertThat(command, "alter async-event-queue --id=test --batch-size=100")
        .statusIsError().containsOutput("Can not find an async event queue");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  @Test
  public void emptyConfiguration_ifExists() throws Exception {
    gfsh.executeAndAssertThat(command,
        "alter async-event-queue --id=test --batch-size=100 --if-exists").statusIsSuccess()
        .containsOutput("Skipping: Can not find an async event queue with id");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  @Test
  public void cluster_config_service_not_available() throws Exception {
    doReturn(null).when(command).getSharedConfiguration();
    gfsh.executeAndAssertThat(command, "alter async-event-queue --id=test --batch-size=100")
        .statusIsError().containsOutput("Cluster Configuration Service is not available");
  }

  @Test
  public void queueIdNotFoundInTheMap() throws Exception {
    Configuration configuration = new Configuration("group");
    configuration.setCacheXmlContent(getCacheXml("queue1", "queue2"));
    configRegion.put("group", configuration);

    gfsh.executeAndAssertThat(command, "alter async-event-queue --batch-size=100 --id=queue")
        .statusIsError().containsOutput("Can not find an async event queue");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  @Test
  public void cannotLockClusterConfiguration() throws Exception {
    when(service.lockSharedConfiguration()).thenReturn(false);
    gfsh.executeAndAssertThat(command, "alter async-event-queue --batch-size=100 --id=queue")
        .statusIsError().containsOutput("Unable to lock the cluster configuration");
  }

  @Test
  public void queueIdFoundInTheMap_updateBatchSize() throws Exception {
    gfsh.executeAndAssertThat(command, "alter async-event-queue --batch-size=100 --id=queue1")
        .statusIsSuccess().tableHasRowCount("Group", 1)
        .tableHasRowWithValues("Group", "Status", "group1", "Cluster Configuration Updated")
        .containsOutput("Please restart the servers");

    // verify that the xml is updated
    Element element =
        findAsyncEventQueueElement(configRegion.get("group1").getCacheXmlContent(), 0);
    assertThat(element.getAttribute(ID)).isEqualTo("queue1");
    assertThat(element.getAttribute(BATCH_SIZE)).isEqualTo("100");
    assertThat(element.getAttribute(BATCH_TIME_INTERVAL)).isEqualTo("");
    assertThat(element.getAttribute(MAX_QUEUE_MEMORY)).isEqualTo("");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  @Test
  public void queueIdFoundInTheMap_updateBatchTimeInterval() throws Exception {
    gfsh.executeAndAssertThat(command,
        "alter async-event-queue --batch-time-interval=100 --id=queue1").statusIsSuccess()
        .tableHasRowCount("Group", 1)
        .tableHasRowWithValues("Group", "Status", "group1", "Cluster Configuration Updated")
        .containsOutput("Please restart the servers");

    // verify that the xml is updated
    Element element =
        findAsyncEventQueueElement(configRegion.get("group1").getCacheXmlContent(), 0);
    assertThat(element.getAttribute(ID)).isEqualTo("queue1");
    assertThat(element.getAttribute(BATCH_SIZE)).isEqualTo("");
    assertThat(element.getAttribute(BATCH_TIME_INTERVAL)).isEqualTo("100");
    assertThat(element.getAttribute(MAX_QUEUE_MEMORY)).isEqualTo("");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  @Test
  public void queueIdFoundInTheMap_updateMaxMemory() throws Exception {
    gfsh.executeAndAssertThat(command, "alter async-event-queue --max-queue-memory=100 --id=queue1")
        .statusIsSuccess().tableHasRowCount("Group", 1)
        .tableHasRowWithValues("Group", "Status", "group1", "Cluster Configuration Updated")
        .containsOutput("Please restart the servers");

    // verify that the xml is updated
    Element element =
        findAsyncEventQueueElement(configRegion.get("group1").getCacheXmlContent(), 0);
    assertThat(element.getAttribute(ID)).isEqualTo("queue1");
    assertThat(element.getAttribute(BATCH_SIZE)).isEqualTo("");
    assertThat(element.getAttribute(BATCH_TIME_INTERVAL)).isEqualTo("");
    assertThat(element.getAttribute(MAXIMUM_QUEUE_MEMORY)).isEqualTo("100");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  @Test
  public void multipleQueuesInClusterConfig() throws Exception {
    when(configRegion.keySet()).thenReturn(Collections.singleton("group"));
    Configuration configuration = new Configuration("group");
    configuration.setCacheXmlContent(getCacheXml("queue1", "queue2"));
    when(configRegion.get("group")).thenReturn(configuration);

    gfsh.executeAndAssertThat(command, "alter async-event-queue --batch-size=100 --id=queue1")
        .statusIsSuccess().tableHasRowCount("Group", 1)
        .tableHasRowWithValues("Group", "Status", "group", "Cluster Configuration Updated")
        .containsOutput("Please restart the servers");

    // verify that queue1's xml is updated
    Element element = findAsyncEventQueueElement(configRegion.get("group").getCacheXmlContent(), 0);
    assertThat(element.getAttribute(ID)).isEqualTo("queue1");
    assertThat(element.getAttribute(BATCH_SIZE)).isEqualTo("100");
    assertThat(element.getAttribute(BATCH_TIME_INTERVAL)).isEqualTo("");
    assertThat(element.getAttribute(MAX_QUEUE_MEMORY)).isEqualTo("");

    // verify that queue2's xml is untouched
    element = findAsyncEventQueueElement(configRegion.get("group").getCacheXmlContent(), 1);
    assertThat(element.getAttribute(ID)).isEqualTo("queue2");
    assertThat(element.getAttribute(BATCH_SIZE)).isEqualTo("");
    assertThat(element.getAttribute(BATCH_TIME_INTERVAL)).isEqualTo("");
    assertThat(element.getAttribute(MAX_QUEUE_MEMORY)).isEqualTo("");

    verify(service).lockSharedConfiguration();
    verify(service).unlockSharedConfiguration();
  }

  private Element findAsyncEventQueueElement(String xml, int index) throws Exception {
    Document document = XmlUtils.createDocumentFromXml(xml);
    NodeList nodeList = document.getElementsByTagName("async-event-queue");
    return (Element) nodeList.item(index);
  }

  private String getAsyncEventQueueXml(String queueId) {
    String xml = "<async-event-queue dispatcher-threads=\"1\" id=\"" + queueId + "\">\n"
        + "    <async-event-listener>\n"
        + "      <class-name>org.apache.geode.internal.cache.wan.MyAsyncEventListener</class-name>\n"
        + "    </async-event-listener>\n" + "  </async-event-queue>\n";
    return xml;
  }

  private String getCacheXml(String... queueIds) {
    String xml = "<cache>\n" + Arrays.stream(queueIds).map(x -> getAsyncEventQueueXml(x))
        .collect(Collectors.joining("\n")) + "</cache>";
    return xml;
  }
}
