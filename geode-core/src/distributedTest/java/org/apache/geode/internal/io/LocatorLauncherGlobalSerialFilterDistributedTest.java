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
package org.apache.geode.internal.io;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@SuppressWarnings("serial")
public class LocatorLauncherGlobalSerialFilterDistributedTest implements Serializable {

  private VM locatorVM;
  private File locatorDir;
  private int locatorPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedReference<LocatorLauncher> locator = new DistributedReference<>();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws IOException {
    locatorVM = getVM(0).bounce();
    locatorDir = temporaryFolder.newFolder("locator");
    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    locatorVM.invoke(() -> {
      locator.set(startLocator("locator", locatorDir, locatorPort));
    });
  }

  @Test
  public void stringIsAllowed() {
    locatorVM.invoke(() -> {
      Serializable object = "hello";
      try (ObjectInput inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
        assertThat(inputStream.readObject()).isEqualTo(object);
      }
    });
  }

  @Test
  public void primitiveIsAllows() {
    locatorVM.invoke(() -> {
      Integer integerObject = 1;
      try (ObjectInput inputStream = new ObjectInputStream(byteArrayInputStream(integerObject))) {
        assertThat(inputStream.readObject()).isEqualTo(integerObject);
      }
    });
  }

  @Test
  public void nonAllowedDoesThrowJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();
    addIgnoredException(InvalidClassException.class);

    locatorVM.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        Serializable object = new SerializableClass("hello");
        try (ObjectInput inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
          assertThat(inputStream.readObject()).isEqualTo(object);
        }
      });
      assertThat(thrown).isInstanceOf(InvalidClassException.class);
    });
  }

  @Test
  public void allowedDoesNotThrowJava9() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();
    // As long as it is not sent within the JMX RMI socket, SerializableClass is allowed.
    locatorVM.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        Serializable object = new SerializableClass("hello");
        try (ObjectInput inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
          assertThat(inputStream.readObject()).isEqualTo(object);
        }
      });
      assertThat(thrown).isNull();
    });
  }

  private LocatorLauncher startLocator(String locatorName, File locatorDir, int locatorPort) {
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setDeletePidFileOnStop(true)
        .setMemberName(locatorName)
        .setWorkingDirectory(locatorDir.getAbsolutePath())
        .setPort(locatorPort)
        .set(ENABLE_CLUSTER_CONFIGURATION, "false")
        .set(LOCATORS, String.format("localhost[%s]", locatorPort))
        .set(JMX_MANAGER, "false")
        .set(HTTP_SERVICE_PORT, "0")
        .set(VALIDATE_SERIALIZABLE_OBJECTS, "true")
        .set(SERIALIZABLE_OBJECT_FILTER,
            "org.apache.geode.internal.io.LocatorLauncherGlobalSerialFilterDistributedTest;org.apache.geode.internal.io.LocatorLauncherGlobalSerialFilterDistributedTest$$Lambda*;org.apache.geode.test.dunit.**")
        .build();

    locatorLauncher.start();

    return locatorLauncher;
  }

  private static ByteArrayInputStream byteArrayInputStream(Serializable object) {
    return new ByteArrayInputStream(serialize(object));
  }

  @SuppressWarnings("all")
  private static class SerializableClass implements Serializable {

    private final String value;

    private SerializableClass(String value) {
      this.value = requireNonNull(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SerializableClass that = (SerializableClass) o;
      return value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return hash(value);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("SerializableClass{");
      sb.append("value='").append(value).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }
}
