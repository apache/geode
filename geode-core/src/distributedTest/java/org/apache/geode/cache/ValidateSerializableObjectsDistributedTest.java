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
package org.apache.geode.cache;

import static java.util.Arrays.asList;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocatorPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.SerializationException;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("serial")
public class ValidateSerializableObjectsDistributedTest implements Serializable {

  private VM server1;
  private VM server2;

  private File server1Dir;
  private File server2Dir;
  private int locatorPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedReference<ServerLauncher> server = new DistributedReference<>();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws IOException {
    server1 = getVM(0);
    server2 = getVM(1);

    server1Dir = temporaryFolder.newFolder("server1");
    server2Dir = temporaryFolder.newFolder("server2");

    locatorPort = getLocatorPort();

    server1.invoke(() -> {
      server.set(startServer("server1", server1Dir));
    });
    server2.invoke(() -> {
      server.set(startServer("server2", server2Dir));
    });

    asList(server1, server2).forEach(vm -> vm.invoke(() -> {
      server.get().getCache()
          .createRegionFactory(REPLICATE)
          .create("region");
    }));
  }

  @Test
  public void regionPutAndGet_forStringKeyAndValue_isAllowedToDeserialize() {
    server1.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");
      region.put("key", "value");
    });

    server2.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");

      int size = region.size();
      assertThat(size).isOne();

      Object value = region.get("key");
      assertThat(value).isEqualTo("value");
    });
  }

  @Test
  public void regionPutAndGet_forPrimitiveKeyAndValue_isAllowedToDeserialize() {
    server1.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");
      region.put(1, 1);
    });

    server2.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");

      int size = region.size();
      assertThat(size).isOne();

      Object value = region.get(1);
      assertThat(value).isEqualTo(1);
    });
  }

  @Test
  public void regionPut_forNonSerializableKeyAndValue_throwsInternalGemFireException_wrappingNotSerializableException() {
    server1.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");
      Throwable thrown = catchThrowable(() -> {
        region.put(new Object(), new Object());
      });
      assertThat(thrown)
          .isInstanceOf(InternalGemFireException.class)
          .hasCauseExactlyInstanceOf(NotSerializableException.class);
    });

    server2.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");

      int size = region.size();
      assertThat(size).isZero();

      Object value = region.get(new Object());
      assertThat(value).isNull();
    });
  }

  @Test
  public void regionGet_forAllowedKeyWithNonAllowedValue_throwsSerializationException_wrappingInvalidClassException() {
    addIgnoredException(InvalidClassException.class);
    addIgnoredException(SerializationException.class);

    server1.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");
      region.put("key", new SerializableClass());
    });

    server2.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");

      int size = region.size();
      assertThat(size).isOne();

      Throwable thrown = catchThrowable(() -> {
        region.get("key");
      });
      assertThat(thrown)
          .isInstanceOf(SerializationException.class)
          .hasCauseInstanceOf(InvalidClassException.class);
    });
  }

  @Test
  public void regionPut_forNonAllowedKeyWithAllowedValue_throwsInternalGemFireException_wrappingIOException_wrappingInvalidClassException() {
    addIgnoredException(InvalidClassException.class);
    addIgnoredException(IOException.class);

    server1.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");
      Throwable thrown = catchThrowable(() -> {
        region.put(new SerializableClass(), "value");
      });
      assertThat(thrown)
          .isInstanceOf(InternalGemFireException.class)
          .hasCauseInstanceOf(IOException.class)
          .hasRootCauseInstanceOf(InvalidClassException.class);
    });

    server2.invoke(() -> {
      Region<Object, Object> region = server.get().getCache().getRegion("region");
      int size = region.size();
      assertThat(size).isZero();
    });
  }

  private ServerLauncher startServer(String serverName, File serverDir) {
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setDisableDefaultServer(true)
        .setDeletePidFileOnStop(true)
        .setMemberName(serverName)
        .setWorkingDirectory(serverDir.getAbsolutePath())
        .setServerPort(0)
        .set(ENABLE_CLUSTER_CONFIGURATION, "false")
        .set(LOCATORS, "localHost[" + locatorPort + "]")
        .set(VALIDATE_SERIALIZABLE_OBJECTS, "true")
        .build();

    serverLauncher.start();

    return serverLauncher;
  }

  public static class SerializableClass implements Serializable {
  }
}
