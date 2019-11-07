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
package org.apache.geode.internal.cache.partitioned.colocation;

import static org.apache.geode.internal.cache.partitioned.colocation.ColocationLoggerFactory.COLOCATION_LOGGER_FACTORY_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.internal.cache.PartitionedRegion;

public class ColocationLoggerFactoryTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void createReturnsColocationLoggerFactoryImplByDefault() {
    ColocationLoggerFactory factory = ColocationLoggerFactory.create();

    assertThat(factory).isInstanceOf(SingleThreadColocationLoggerFactory.class);
  }

  @Test
  public void createReturnsInstanceOfClassSpecifiedBySystemProperty() {
    System.setProperty(COLOCATION_LOGGER_FACTORY_PROPERTY,
        MyColocationLoggerFactory.class.getName());

    ColocationLoggerFactory factory = ColocationLoggerFactory.create();

    assertThat(factory).isInstanceOf(MyColocationLoggerFactory.class);
  }

  @Test
  public void createReturnsDefaultIfSystemPropertyValueDoesNotExist() {
    System.setProperty(COLOCATION_LOGGER_FACTORY_PROPERTY, "does.not.Exist");

    ColocationLoggerFactory factory = ColocationLoggerFactory.create();

    assertThat(factory).isInstanceOf(SingleThreadColocationLoggerFactory.class);
  }

  @Test
  public void createReturnsDefaultIfSystemPropertyValueDoesNotImplementInterface() {
    System.setProperty(COLOCATION_LOGGER_FACTORY_PROPERTY,
        DoesNotImplementInterface.class.getName());

    ColocationLoggerFactory factory = ColocationLoggerFactory.create();

    assertThat(factory).isInstanceOf(SingleThreadColocationLoggerFactory.class);
  }

  @Test
  public void createReturnsDefaultIfSystemPropertyValueIsPrivateClass() {
    System.setProperty(COLOCATION_LOGGER_FACTORY_PROPERTY, PrivateFactory.class.getName());

    ColocationLoggerFactory factory = ColocationLoggerFactory.create();

    assertThat(factory).isInstanceOf(SingleThreadColocationLoggerFactory.class);
  }

  /**
   * Must be package-private.
   */
  static class MyColocationLoggerFactory implements ColocationLoggerFactory {

    @Override
    public ColocationLogger startColocationLogger(PartitionedRegion region) {
      return null;
    }
  }

  /**
   * Must be package-private.
   */
  static class DoesNotImplementInterface {
    // nothing
  }

  /**
   * Must be private.
   */
  private static class PrivateFactory implements ColocationLoggerFactory {

    @Override
    public ColocationLogger startColocationLogger(PartitionedRegion region) {
      return null;
    }
  }
}
