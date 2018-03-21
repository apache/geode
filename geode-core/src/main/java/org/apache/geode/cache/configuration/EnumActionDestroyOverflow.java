
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

package org.apache.geode.cache.configuration;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 * <p>
 * Java class for enum-action-destroy-overflow.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 * <p>
 *
 * <pre>
 * &lt;simpleType name="enum-action-destroy-overflow">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="local-destroy"/>
 *     &lt;enumeration value="overflow-to-disk"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 *
 */
@XmlType(name = "enum-action-destroy-overflow", namespace = "http://geode.apache.org/schema/cache")
@XmlEnum
@Experimental
public enum EnumActionDestroyOverflow {

  @XmlEnumValue("local-destroy")
  LOCAL_DESTROY("local-destroy"), @XmlEnumValue("overflow-to-disk")
  OVERFLOW_TO_DISK("overflow-to-disk");
  private final String value;

  EnumActionDestroyOverflow(String v) {
    value = v;
  }

  public String value() {
    return value;
  }

  public static EnumActionDestroyOverflow fromValue(String v) {
    for (EnumActionDestroyOverflow c : EnumActionDestroyOverflow.values()) {
      if (c.value.equals(v)) {
        return c;
      }
    }
    throw new IllegalArgumentException(v);
  }

}
