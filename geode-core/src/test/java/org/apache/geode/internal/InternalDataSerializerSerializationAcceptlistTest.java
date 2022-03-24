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
package org.apache.geode.internal;

import static java.util.Collections.emptySet;
import static org.apache.geode.distributed.internal.DistributionConfig.SERIALIZABLE_OBJECT_FILTER_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.VALIDATE_SERIALIZABLE_OBJECTS_NAME;
import static org.apache.geode.internal.lang.ClassUtils.isClassAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class InternalDataSerializerSerializationAcceptlistTest {

  private HeapDataOutputStream outputStream;
  private Object testSerializable;
  private Properties properties;

  @BeforeClass
  public static void hasObjectInputFilter() {
    assumeTrue("ObjectInputFilter is present in this JVM",
        isClassAvailable("sun.misc.ObjectInputFilter") ||
            isClassAvailable("java.io.ObjectInputFilter"));
  }

  @Before
  public void setUp() {
    outputStream = new HeapDataOutputStream(KnownVersion.CURRENT);
    testSerializable = new TestSerializable();
    properties = new Properties();
  }

  @After
  public void clearSerializationFilter() {
    InternalDataSerializer.clearSerializationFilter();
  }

  @Test
  public void distributionConfigDefaults() {
    DistributionConfig distributionConfig = new DistributionConfigImpl(new Properties());

    assertThat(distributionConfig.getValidateSerializableObjects()).isFalse();
    assertThat(distributionConfig.getSerializableObjectFilter()).isEqualTo("!*");
  }

  @Test
  public void canSerializeWhenFilterIsDisabled() throws Exception {
    trySerializingTestObject(new Properties());
  }

  @Test
  public void notAcceptlistedWithFilterCannotSerialize() {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");

    Throwable thrown = catchThrowable(() -> {
      trySerializingTestObject(properties);
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithFilterCanSerialize() throws Exception {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, TestSerializable.class.getName());

    trySerializingTestObject(properties);
  }

  @Test
  public void acceptlistedWithNonMatchingFilterCannotSerialize() {
    Throwable thrown = catchThrowable(() -> {
      trySerializingWithFilter("RabidMonkeyTurnip");
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithPartialMatchingFilterCannotSerialize() {
    Throwable thrown = catchThrowable(() -> {
      trySerializingWithFilter("TestSerializable"); // Not fully qualified class name
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithEmptyFilterCannotSerialize() {
    Throwable thrown = catchThrowable(() -> {
      trySerializingWithFilter("");
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithIncorrectPathFilterCannotSerialize() {
    Throwable thrown = catchThrowable(() -> {
      trySerializingWithFilter(
          "org.apache.commons.InternalDataSerializerSerializationAcceptlistTest$TestSerializable");
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithWildcardPathFilterCannotSerialize() {
    Throwable thrown = catchThrowable(() -> {
      trySerializingWithFilter("org.apache.*");
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithWildcardSubpathFilterCanSerialize() throws Exception {
    trySerializingWithFilter("org.apache.**");
  }

  private void trySerializingWithFilter(String filter) throws Exception {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, filter);

    trySerializingTestObject(properties);
  }

  private void trySerializingTestObject(Properties properties)
      throws IOException, ClassNotFoundException {
    DistributionConfig distributionConfig = new DistributionConfigImpl(properties);
    InternalDataSerializer.initializeSerializationFilter(distributionConfig, emptySet());

    DataSerializer.writeObject(testSerializable, outputStream);

    // if this throws, we're good!
    DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
  }

  @SuppressWarnings("serial")
  private static class TestSerializable implements Serializable {
    // nothing
  }
}
