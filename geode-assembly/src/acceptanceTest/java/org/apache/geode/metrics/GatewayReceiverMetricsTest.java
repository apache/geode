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

package org.apache.geode.metrics;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import io.micrometer.core.instrument.Counter;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.categories.MetricsTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(MetricsTest.class)
public class GatewayReceiverMetricsTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String SENDER_LOCATOR_NAME = "sender-locator";
  private static final String RECEIVER_LOCATOR_NAME = "receiver-locator";
  private static final String SENDER_SERVER_NAME = "sender-server";
  private static final String RECEIVER_SERVER_NAME = "receiver-server";
  private static final String REGION_NAME = "region";
  private static final String GFSH_COMMAND_SEPARATOR = " ";
  private String senderLocatorFolder;
  private String receiverLocatorFolder;
  private String senderServerFolder;
  private String receiverServerFolder;
  private int receiverLocatorPort;
  private int senderLocatorPort;

  @Before
  public void startClusters() throws IOException {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(8);

    receiverLocatorPort = ports[0];
    senderLocatorPort = ports[1];
    int senderServerPort = ports[2];
    int receiverServerPort = ports[3];
    int senderLocatorJmxPort = ports[4];
    int receiverLocatorJmxPort = ports[5];
    int senderLocatorHttpPort = ports[6];
    int receiverLocatorHttpPort = ports[7];

    int senderSystemId = 2;
    int receiverSystemId = 1;

    senderLocatorFolder = newFolder(SENDER_LOCATOR_NAME);
    receiverLocatorFolder = newFolder(RECEIVER_LOCATOR_NAME);
    senderServerFolder = newFolder(SENDER_SERVER_NAME);
    receiverServerFolder = newFolder(RECEIVER_SERVER_NAME);

    String startSenderLocatorCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start locator",
        "--name=" + SENDER_LOCATOR_NAME,
        "--dir=" + senderLocatorFolder,
        "--port=" + senderLocatorPort,
        "--locators=localhost[" + senderLocatorPort + "]",
        "--J=-Dgemfire.remote-locators=localhost[" + receiverLocatorPort + "]",
        "--J=-Dgemfire.distributed-system-id=" + senderSystemId,
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + senderLocatorHttpPort,
        "--J=-Dgemfire.jmx-manager-port=" + senderLocatorJmxPort);

    String startReceiverLocatorCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start locator",
        "--name=" + RECEIVER_LOCATOR_NAME,
        "--dir=" + receiverLocatorFolder,
        "--port=" + receiverLocatorPort,
        "--locators=localhost[" + receiverLocatorPort + "]",
        "--J=-Dgemfire.remote-locators=localhost[" + senderLocatorPort + "]",
        "--J=-Dgemfire.distributed-system-id=" + receiverSystemId,
        "--J=-Dgemfire.jmx-manager-start=true ",
        "--J=-Dgemfire.jmx-manager-http-port=" + receiverLocatorHttpPort,
        "--J=-Dgemfire.jmx-manager-port=" + receiverLocatorJmxPort);

    String startSenderServerCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start server",
        "--name=" + SENDER_SERVER_NAME,
        "--dir=" + senderServerFolder,
        "--locators=localhost[" + senderLocatorPort + "]",
        "--server-port=" + senderServerPort,
        "--J=-Dgemfire.distributed-system-id=" + senderSystemId);

    String metricsPublishingServiceJarPath =
        newJarForMetricsPublishingServiceClass(SimpleMetricsPublishingService.class,
            "metrics-publishing-service.jar");

    String startReceiverServerCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "start server",
        "--name=" + RECEIVER_SERVER_NAME,
        "--dir=" + receiverServerFolder,
        "--locators=localhost[" + receiverLocatorPort + "]",
        "--server-port=" + receiverServerPort,
        "--classpath=" + metricsPublishingServiceJarPath,
        "--J=-Dgemfire.distributed-system-id=" + receiverSystemId);

    gfshRule.execute(startSenderLocatorCommand, startReceiverLocatorCommand,
        startSenderServerCommand, startReceiverServerCommand);

    String gatewaySenderId = "gs";

    String connectToSenderLocatorCommand = "connect --locator=localhost[" + senderLocatorPort + "]";

    String startGatewaySenderCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create gateway-sender",
        "--id=" + gatewaySenderId,
        "--parallel=false",
        "--remote-distributed-system-id=" + receiverSystemId);

    String createSenderRegionCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create region",
        "--name=" + REGION_NAME,
        "--type=" + RegionShortcut.REPLICATE.name(),
        "--gateway-sender-id=" + gatewaySenderId);

    gfshRule.execute(connectToSenderLocatorCommand, startGatewaySenderCommand,
        createSenderRegionCommand);

    String connectToReceiverLocatorCommand =
        "connect --locator=localhost[" + receiverLocatorPort + "]";
    String startGatewayReceiverCommand = "create gateway-receiver";
    String createReceiverRegionCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create region",
        "--name=" + REGION_NAME,
        "--type=" + RegionShortcut.REPLICATE.name());

    gfshRule.execute(connectToReceiverLocatorCommand, startGatewayReceiverCommand,
        createReceiverRegionCommand);

    // Deploy function to members
    String functionJarPath =
        newJarForFunctionClass(GetEventsReceivedCountFunction.class, "function.jar");
    String deployCommand = "deploy --jar=" + functionJarPath;
    String listFunctionsCommand = "list functions";

    gfshRule.execute(connectToReceiverLocatorCommand, deployCommand, listFunctionsCommand);
  }

  @After
  public void stopClusters() {
    String stopReceiverServerCommand = "stop server --dir=" + receiverServerFolder;
    String stopSenderServerCommand = "stop server --dir=" + senderServerFolder;
    String stopReceiverLocatorCommand = "stop locator --dir=" + receiverLocatorFolder;
    String stopSenderLocatorCommand = "stop locator --dir=" + senderLocatorFolder;

    gfshRule.execute(stopReceiverServerCommand, stopSenderServerCommand, stopReceiverLocatorCommand,
        stopSenderLocatorCommand);
  }

  @Test
  public void whenPerformingOperations_thenGatewayReceiverEventsReceivedIncreases() {
    String connectToSenderLocatorCommand = "connect --locator=localhost[" + senderLocatorPort + "]";

    String doPutCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "put",
        "--region=" + REGION_NAME,
        "--key=foo",
        "--value=bar");

    String doRemoveCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "remove",
        "--region=" + REGION_NAME,
        "--key=foo");

    String doCreateRegionCommand = String.join(GFSH_COMMAND_SEPARATOR,
        "create region",
        "--name=blah",
        "--type=" + RegionShortcut.REPLICATE.name());

    gfshRule.execute(connectToSenderLocatorCommand, doPutCommand, doRemoveCommand,
        doCreateRegionCommand);

    String connectToReceiverLocatorCommand =
        "connect --locator=localhost[" + receiverLocatorPort + "]";
    String executeFunctionCommand = "execute function --id=" + GetEventsReceivedCountFunction.ID;

    Collection<String> gatewayEventsExpectedToReceive =
        Arrays.asList(doPutCommand, doRemoveCommand);

    await().untilAsserted(() -> {
      String output =
          gfshRule.execute(connectToReceiverLocatorCommand, executeFunctionCommand).getOutputText();

      assertThat(output.trim())
          .as("Returned count of events received.")
          .endsWith("[" + gatewayEventsExpectedToReceive.size() + ".0]");
    });
  }

  private String newFolder(String folderName) throws IOException {
    return temporaryFolder.newFolder(folderName).getAbsolutePath();
  }

  private String newJarForFunctionClass(Class clazz, String jarName) throws IOException {
    File jar = temporaryFolder.newFile(jarName);
    new ClassBuilder().writeJarFromClass(clazz, jar);
    return jar.getAbsolutePath();
  }

  private String newJarForMetricsPublishingServiceClass(Class clazz, String jarName)
      throws IOException {
    File jar = temporaryFolder.newFile(jarName);

    String className = clazz.getName();
    String classAsPath = className.replace('.', '/') + ".class";
    InputStream stream = clazz.getClassLoader().getResourceAsStream(classAsPath);
    byte[] bytes = IOUtils.toByteArray(stream);
    try (FileOutputStream out = new FileOutputStream(jar)) {
      JarOutputStream jarOutputStream = new JarOutputStream(out);

      // Add the class file to the JAR file
      JarEntry classEntry = new JarEntry(classAsPath);
      classEntry.setTime(System.currentTimeMillis());
      jarOutputStream.putNextEntry(classEntry);
      jarOutputStream.write(bytes);
      jarOutputStream.closeEntry();

      String metaInfPath = "META-INF/services/org.apache.geode.metrics.MetricsPublishingService";

      JarEntry metaInfEntry = new JarEntry(metaInfPath);
      metaInfEntry.setTime(System.currentTimeMillis());
      jarOutputStream.putNextEntry(metaInfEntry);
      jarOutputStream.write(className.getBytes());
      jarOutputStream.closeEntry();

      jarOutputStream.close();
    }

    return jar.getAbsolutePath();
  }

  public static class GetEventsReceivedCountFunction implements Function<Void> {
    static final String ID = "GetEventsReceivedCountFunction";

    @Override
    public void execute(FunctionContext<Void> context) {
      Counter eventsReceivedCounter = SimpleMetricsPublishingService.getRegistry()
          .find("cache.gatewayreceiver.events.received")
          .counter();

      Object result = eventsReceivedCounter == null
          ? "Meter not found."
          : eventsReceivedCounter.count();

      context.getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
