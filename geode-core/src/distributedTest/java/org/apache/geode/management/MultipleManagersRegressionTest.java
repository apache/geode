/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Arrays.asList;
import static javax.management.ObjectName.getInstance;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(JMXTest.class)
@SuppressWarnings("serial")
public class MultipleManagersRegressionTest implements Serializable {

  private static final LocatorLauncher DUMMY_LOCATOR = mock(LocatorLauncher.class);

  private static final AtomicReference<LocatorLauncher> LOCATOR =
      new AtomicReference<>(DUMMY_LOCATOR);

  private VM locator1VM;
  private VM locator2VM;

  private String locator1Name;
  private String locator2Name;
  private String locators;
  private int locator1Port;
  private int locator2Port;
  private int locator1JmxPort;
  private int locator2JmxPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    locator1VM = getVM(1);
    locator2VM = getVM(-1);

    locator1Name = "locator1";
    locator2Name = "locator2";
    File locator1Dir = temporaryFolder.newFolder(locator1Name);
    File locator2Dir = temporaryFolder.newFolder(locator2Name);

    int[] port = getRandomAvailableTCPPorts(4);
    locator1Port = port[0];
    locator2Port = port[1];
    locator1JmxPort = port[2];
    locator2JmxPort = port[3];
    locators = "localhost[" + locator1Port + "],localhost[" + locator2Port + "]";

    locator1VM.invoke(() -> {
      startLocator(locator1Name, locator1Dir, locator1Port, locator1JmxPort, locators);
    });
    locator2VM.invoke(() -> {
      startLocator(locator2Name, locator2Dir, locator2Port, locator2JmxPort, locators);
    });
  }

  @After
  public void tearDown() {
    for (VM vm : asList(locator2VM, locator1VM)) {
      vm.invoke(() -> {
        LOCATOR.getAndSet(DUMMY_LOCATOR).stop();
      });
    }
  }

  @Test
  public void locatorHasMemberTypeMXBeansForBothLocators() {
    locator1VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator-" + getVMId())
            .containsAll(expectedLocatorMXBeans(locator1Name))
            .containsAll(expectedLocatorMXBeans(locator2Name))
            .containsAll(expectedDistributedMXBeans());
      });
    });

    locator2VM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(getPlatformMBeanServer().queryNames(getInstance("GemFire:*"), null))
            .as("GemFire mbeans on locator-" + getVMId())
            .containsAll(expectedLocatorMXBeans(locator2Name))
            .containsAll(expectedLocatorMXBeans(locator1Name))
            .containsAll(expectedDistributedMXBeans());
      });
    });
  }

  private static void startLocator(String name, File workingDirectory, int locatorPort, int jmxPort,
      String locators) {
    LOCATOR.set(new LocatorLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setMemberName(name)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER, "true")
        .set(JMX_MANAGER_PORT, String.valueOf(jmxPort))
        .set(JMX_MANAGER_START, "true")
        .set(LOCATORS, locators)
        .set(LOG_FILE, new File(workingDirectory, name + ".log").getAbsolutePath())
        .set(MAX_WAIT_TIME_RECONNECT, "1000")
        .set(MEMBER_TIMEOUT, "2000")
        .build());

    LOCATOR.get().start();

    await().untilAsserted(() -> {
      InternalLocator locator = (InternalLocator) LOCATOR.get().getLocator();
      assertThat(locator.isSharedConfigurationRunning())
          .as("Locator shared configuration is running on locator-" + getVMId())
          .isTrue();
    });
  }

  private static Set<ObjectName> expectedLocatorMXBeans(String memberName)
      throws MalformedObjectNameException {
    return new HashSet<>(asList(
        getInstance("GemFire:service=DiskStore,name=cluster_config,type=Member,member=" +
            memberName),
        getInstance("GemFire:service=Locator,type=Member,member=" + memberName),
        getInstance("GemFire:service=LockService,name=__CLUSTER_CONFIG_LS,type=Member,member=" +
            memberName),
        getInstance("GemFire:type=Member,member=" + memberName),
        getInstance("GemFire:service=Manager,type=Member,member=" + memberName)));
  }

  private static Set<ObjectName> expectedDistributedMXBeans()
      throws MalformedObjectNameException {
    return new HashSet<>(asList(
        getInstance("GemFire:service=AccessControl,type=Distributed"),
        getInstance("GemFire:service=FileUploader,type=Distributed"),
        getInstance("GemFire:service=LockService,name=__CLUSTER_CONFIG_LS,type=Distributed"),
        getInstance("GemFire:service=System,type=Distributed")));
  }
}
