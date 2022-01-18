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
package org.apache.geode.internal.serialization.filter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.rmi.MarshalledObject;
import java.util.StringJoiner;

import javax.management.ObjectName;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularType;

/**
 * Defines a serial filter pattern that accepts all open MBean data types and rejects everything
 * not included in the pattern.
 */
public class OpenMBeanFilterPattern implements FilterPattern {

  @Override
  public String pattern() {
    // note: java.util.* may also be needed
    return new StringJoiner(";")

        // accept all open MBean data types
        .add(Boolean.class.getName())
        .add(Byte.class.getName())
        .add(Character.class.getName())
        .add(Short.class.getName())
        .add(Integer.class.getName())
        .add(Long.class.getName())
        .add(Float.class.getName())
        .add(Double.class.getName())
        .add(String.class.getName())
        .add(BigInteger.class.getName())
        .add(BigDecimal.class.getName())
        .add(ObjectName.class.getName())
        .add(OpenType.class.getName())
        .add(CompositeData.class.getName())
        .add(TabularData.class.getName())
        .add(SimpleType.class.getName())
        .add(CompositeType.class.getName())
        .add(TabularType.class.getName())
        .add(ArrayType.class.getName())
        .add(MarshalledObject.class.getName())

        // reject all others
        .add("!*")
        .toString();
  }
}
