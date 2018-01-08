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

package org.apache.geode.distributed;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.gms.NetLocator;
import org.apache.geode.distributed.internal.membership.gms.locator.GMSLocator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocatorViewFileTemporaryFolderDUnitTest implements Serializable {

  private static volatile InternalLocator internalLocator;
  private static volatile String previousViewFilePath;

  private volatile String currentViewFilePath;

  private transient VM locatorVM;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    locatorVM = Host.getHost(0).getVM(0);
    String locatorFolder = "vm-" + locatorVM.getId() + "-" + testName.getMethodName();

    int port = locatorVM.invoke(() -> {
      System.setProperty("user.dir", temporaryFolder.newFolder(locatorFolder).getAbsolutePath());
      Properties config = new Properties();
      config.setProperty(LOCATORS, "");
      internalLocator = (InternalLocator) Locator.startLocatorAndDS(0, null, config);
      await().atMost(2, MINUTES)
          .until(() -> assertTrue(internalLocator.isSharedConfigurationRunning()));
      return Locator.getLocator().getPort();
    });

    assertThat(port).isGreaterThan(0);
  }

  @After
  public void after() {
    Invoke.invokeInEveryVM(() -> {
      if (internalLocator != null) {
        internalLocator.stop();
        internalLocator = null;
      }
    });
  }

  @AfterClass
  public static void afterClass() {
    previousViewFilePath = null;
  }

  @Test
  public void test001() {
    currentViewFilePath = locatorVM.invoke(() -> {
      NetLocator netLocator = internalLocator.getLocatorHandler();
      GMSLocator gmsLocator = (GMSLocator) netLocator;

      File viewFile = gmsLocator.getViewFile();
      assertThat(new File(viewFile.getAbsolutePath())).exists();

      internalLocator.stop();

      return viewFile.getAbsolutePath();
    });

    assertThat(new File(currentViewFilePath)).exists();

    previousViewFilePath = currentViewFilePath;
  }

  @Test
  public void test002() {
    File previousFile = new File(previousViewFilePath);
    assertThat(previousFile).doesNotExist();

    currentViewFilePath = locatorVM.invoke(() -> {
      NetLocator netLocator = internalLocator.getLocatorHandler();
      GMSLocator gmsLocator = (GMSLocator) netLocator;

      File viewFile = gmsLocator.getViewFile();
      assertThat(new File(viewFile.getAbsolutePath())).exists();

      internalLocator.stop();

      return viewFile.getAbsolutePath();
    });

    assertThat(new File(currentViewFilePath)).exists();
    assertThat(currentViewFilePath).isNotEqualTo(previousViewFilePath);
  }
}
