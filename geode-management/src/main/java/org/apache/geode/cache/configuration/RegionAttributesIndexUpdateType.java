
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

import java.io.Serializable;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 * <p>
 * Java class for region-attributesIndex-update-type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 * <p>
 *
 * <pre>
 * &lt;simpleType name="region-attributesIndex-update-type">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="asynchronous"/>
 *     &lt;enumeration value="synchronous"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 *
 */
@XmlType(name = "region-attributesIndex-update-type",
    namespace = "http://geode.apache.org/schema/cache")
@XmlEnum
@Experimental
public enum RegionAttributesIndexUpdateType implements Serializable {

  @XmlEnumValue("asynchronous")
  ASYNCHRONOUS("asynchronous"), @XmlEnumValue("synchronous")
  SYNCHRONOUS("synchronous");
  private final String value;

  RegionAttributesIndexUpdateType(String v) {
    value = v;
  }

  public String value() {
    return value;
  }

  public static RegionAttributesIndexUpdateType fromValue(String v) {
    for (RegionAttributesIndexUpdateType c : RegionAttributesIndexUpdateType.values()) {
      if (c.value.equals(v)) {
        return c;
      }
    }
    throw new IllegalArgumentException(v);
  }

}
