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
package org.apache.geode.management.internal;

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.rmi.MarshalledObject;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.management.ObjectName;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularType;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Configure the “jmx.remote.rmi.server.serial.filter.pattern” system property if Java version is
 * greater than Java 8. The serial pattern will be configured to accept only standard JMX
 * open-types. If the system property already has a non-null value, then leave it as is.
 */
public class JmxRmiOpenTypesSerialFilter implements JmxRmiSerialFilter {

  private static final Logger logger = LogService.getLogger();

  @VisibleForTesting
  public static final String PROPERTY_NAME = "jmx.remote.rmi.server.serial.filter.pattern";

  private final Consumer<String> infoLogger;
  private final Supplier<Boolean> supportsSerialFilter;

  JmxRmiOpenTypesSerialFilter() {
    this(logger::info, () -> isJavaVersionAtLeast(JavaVersion.JAVA_9));
  }

  @VisibleForTesting
  JmxRmiOpenTypesSerialFilter(Consumer<String> infoLogger, Supplier<Boolean> supportsSerialFilter) {
    this.infoLogger = infoLogger;
    this.supportsSerialFilter = supportsSerialFilter;
  }

  @Override
  public void configureSerialFilter() {
    if (supportsDedicatedSerialFilter()) {
      setPropertyValueUnlessExists(createSerialFilterPattern());
    }
  }

  @VisibleForTesting
  boolean supportsDedicatedSerialFilter() {
    return supportsSerialFilter.get();
  }

  /**
   * Sets the value of the system property {@code jmx.remote.rmi.server.serial.filter.pattern}
   * unless it exists with a value that is not null or empty ("").
   */
  @VisibleForTesting
  void setPropertyValueUnlessExists(String value) {
    String existingValue = System.getProperty(PROPERTY_NAME);
    if (StringUtils.isNotEmpty(existingValue)) {
      infoLogger.accept("System property " + PROPERTY_NAME + " is already configured.");
      return;
    }

    System.setProperty(PROPERTY_NAME, value);
    infoLogger.accept("System property " + PROPERTY_NAME + " is now configured with '"
        + value + "'.");
  }

  /**
   * Returns a serial filter pattern that accepts all open MBean data types and rejects everything
   * not included in the pattern.
   */
  @VisibleForTesting
  String createSerialFilterPattern() {
    // note: java.util.* may also be needed
    return new StringBuilder()
        // accept all open MBean data types
        .append(Boolean.class.getName())
        .append(';')
        .append(Byte.class.getName())
        .append(';')
        .append(Character.class.getName())
        .append(';')
        .append(Short.class.getName())
        .append(';')
        .append(Integer.class.getName())
        .append(';')
        .append(Long.class.getName())
        .append(';')
        .append(Float.class.getName())
        .append(';')
        .append(Double.class.getName())
        .append(';')
        .append(String.class.getName())
        .append(';')
        .append(BigInteger.class.getName())
        .append(';')
        .append(BigDecimal.class.getName())
        .append(';')
        .append(ObjectName.class.getName())
        .append(';')
        .append(OpenType.class.getName())
        .append(';')
        .append(CompositeData.class.getName())
        .append(';')
        .append(TabularData.class.getName())
        .append(';')
        .append(SimpleType.class.getName())
        .append(';')
        .append(CompositeType.class.getName())
        .append(';')
        .append(TabularType.class.getName())
        .append(';')
        .append(ArrayType.class.getName())
        .append(';')
        .append(MarshalledObject.class.getName())
        .append(';')
        // reject all other classes
        .append("!*")
        .toString();
  }
}
