package org.apache.geode.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.test.junit.categories.UnitTest;

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
@Category(UnitTest.class)
public class InternalDataSerializerSerializationWhitelistTest {
  private HeapDataOutputStream outputStream;
  private Object testSerializable;
  private Properties properties;

  @Before
  public void setUp() {
    Assume.assumeTrue("ObjectInputFilter is present in this JVM (post- 8.111)",
        hasObjectInputFilter());
    outputStream = new HeapDataOutputStream(Version.CURRENT);
    testSerializable = new TestSerializable();
    properties = new Properties();
  }

  private boolean hasObjectInputFilter() {
    try {
      Class.forName("sun.misc.ObjectInputFilter");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @AfterClass
  public static void clearDataSerializerFilter() {
    InternalDataSerializer.initialize(new DistributionConfigImpl(new Properties()),
        new ArrayList<>());
  }

  @Test
  public void distributionConfigDefaults() {
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(new Properties());

    assertFalse(distributionConfig.getValidateSerializableObjects());
    assertEquals("!*", distributionConfig.getSerializableObjectFilter());
  }

  @Test
  public void canSerializeWhenFilterIsDisabled() throws Exception {
    trySerializingTestObject(new Properties());
  }

  @Test(expected = java.io.InvalidClassException.class)
  public void notWhitelistedWithFilterCannotSerialize() throws Exception {
    properties.setProperty(DistributionConfig.VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");

    trySerializingTestObject(properties);
  }

  @Test
  public void whitelistedWithFilterCanSerialize() throws Exception {
    properties.setProperty(DistributionConfig.VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(DistributionConfig.SERIALIZABLE_OBJECT_FILTER_NAME,
        TestSerializable.class.getName());

    trySerializingTestObject(properties);
  }

  @Test(expected = java.io.InvalidClassException.class)
  public void whitelistedWithNonMatchingFilterCannotSerialize() throws Exception {
    trySerializingWithFilter("RabidMonkeyTurnip");
  }

  @Test(expected = java.io.InvalidClassException.class)
  public void whitelistedWithPartialMatchingFilterCannotSerialize() throws Exception {
    trySerializingWithFilter("TestSerializable"); // Not fully qualified class name
  }

  @Test(expected = java.io.InvalidClassException.class)
  public void whitelistedWithEmptyFilterCannotSerialize() throws Exception {
    trySerializingWithFilter("");
  }

  @Test(expected = java.io.InvalidClassException.class)
  public void whitelistedWithIncorrectPathFilterCannotSerialize() throws Exception {
    trySerializingWithFilter(
        "org.apache.commons.InternalDataSerializerSerializationWhitelistTest$TestSerializable");
  }

  @Test(expected = java.io.InvalidClassException.class)
  public void whitelistedWithWildcardPathFilterCannotSerialize() throws Exception {
    trySerializingWithFilter("org.apache.*");
  }

  @Test
  public void whitelistedWithWildcardSubpathFilterCanSerialize() throws Exception {
    trySerializingWithFilter("org.apache.**");
  }

  private void trySerializingWithFilter(String filter) throws Exception {
    properties.setProperty(DistributionConfig.VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    properties.setProperty(DistributionConfig.SERIALIZABLE_OBJECT_FILTER_NAME, filter);

    trySerializingTestObject(properties);
  }

  private void trySerializingTestObject(Properties properties)
      throws IOException, ClassNotFoundException {
    DistributionConfig distributionConfig = new DistributionConfigImpl(properties);
    InternalDataSerializer.initialize(distributionConfig, new ArrayList<>());

    DataSerializer.writeObject(testSerializable, outputStream);

    // if this throws, we're good!
    DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
  }

  private static class TestSerializable implements Serializable {

  }

}
