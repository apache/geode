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
import static org.apache.geode.internal.InternalDataSerializer.initializeSerializationFilter;
import static org.apache.geode.internal.lang.ClassUtils.isClassAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.Properties;

import org.junit.AfterClass;
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
    boolean isObjectInputFilterClassAvailable =
        isClassAvailable("java.io.ObjectInputFilter") ||
            isClassAvailable("sun.misc.ObjectInputFilter");
    assertThat(isObjectInputFilterClassAvailable)
        .as("java.io.ObjectInputFilter or sun.misc.ObjectInputFilter is available")
        .isTrue();
  }

  @Before
  public void setUp() {
    System.out.println("JC debug jdk: " + System.getProperty("java.version"));
    outputStream = new HeapDataOutputStream(KnownVersion.CURRENT);
    testSerializable = new TestSerializable();
    properties = new Properties();
  }

  @AfterClass
  public static void clearDataSerializerFilter() {
    initializeSerializationFilter(new DistributionConfigImpl(new Properties()), emptySet());
  }

  @Test
  public void distributionConfigDefaults() {
    DistributionConfig distributionConfig = new DistributionConfigImpl(new Properties());

    assertThat(distributionConfig.getValidateSerializableObjects()).isFalse();
    assertThat(distributionConfig.getSerializableObjectFilter()).isEqualTo("!*");
  }

  @Test
  public void canSerializeWhenFilterIsDisabled() throws ClassNotFoundException, IOException {
    initializeSerializationFilter(new DistributionConfigImpl(new Properties()), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Object deserializedObject;
    try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(in)) {
      deserializedObject = DataSerializer.readObject(dataInputStream);
    }

    assertThat(deserializedObject).isInstanceOf(testSerializable.getClass());
  }

  @Test
  public void notAcceptlistedWithFilterCannotSerialize() throws IOException {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Throwable thrown = catchThrowable(() -> {
      try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
          DataInputStream dataInputStream = new DataInputStream(in)) {
        DataSerializer.readObject(dataInputStream);
      }
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithFilterCanSerialize() throws Exception {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, TestSerializable.class.getName());
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Object deserializedObject;
    try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(in)) {
      deserializedObject = DataSerializer.readObject(dataInputStream);
    }

    assertThat(deserializedObject).isInstanceOf(testSerializable.getClass());
  }

  @Test
  public void acceptlistedWithNonMatchingFilterCannotSerialize() throws IOException {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, "RabidMonkeyTurnip");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Throwable thrown = catchThrowable(() -> {
      try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
          DataInputStream dataInputStream = new DataInputStream(in)) {
        DataSerializer.readObject(dataInputStream);
      }
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithPartialMatchingFilterCannotSerialize() throws IOException {
    // Not fully qualified class name
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, "TestSerializable");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Throwable thrown = catchThrowable(() -> {
      try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
          DataInputStream dataInputStream = new DataInputStream(in)) {
        DataSerializer.readObject(dataInputStream);
      }
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithEmptyFilterCannotSerialize() throws IOException {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, "");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Throwable thrown = catchThrowable(() -> {
      try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
          DataInputStream dataInputStream = new DataInputStream(in)) {
        DataSerializer.readObject(dataInputStream);
      }
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithIncorrectPathFilterCannotSerialize() throws IOException {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME,
        "org.apache.commons.InternalDataSerializerSerializationAcceptlistTest$TestSerializable");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Throwable thrown = catchThrowable(() -> {
      try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
          DataInputStream dataInputStream = new DataInputStream(in)) {
        DataSerializer.readObject(dataInputStream);
      }
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithWildcardPathFilterCannotSerialize() throws IOException {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, "org.apache.*");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Throwable thrown = catchThrowable(() -> {
      try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
          DataInputStream dataInputStream = new DataInputStream(in)) {
        DataSerializer.readObject(dataInputStream);
      }
    });

    assertThat(thrown).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void acceptlistedWithWildcardSubpathFilterCanSerialize() throws Exception {
    properties.setProperty(VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(SERIALIZABLE_OBJECT_FILTER_NAME, "org.apache.**");
    initializeSerializationFilter(new DistributionConfigImpl(properties), emptySet());
    DataSerializer.writeObject(testSerializable, outputStream);

    Object deserializedObject;
    try (InputStream in = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(in)) {
      deserializedObject = DataSerializer.readObject(dataInputStream);
    }

    assertThat(deserializedObject).isInstanceOf(testSerializable.getClass());
  }

  @SuppressWarnings("serial")
  private static class TestSerializable implements Serializable {
    // nothing
  }
}
