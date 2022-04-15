
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

package org.apache.geode.cache.configuration;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 * <p>
 * Java class for region-attributesData-policy.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;simpleType name="region-attributesData-policy"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="empty"/&gt;
 *     &lt;enumeration value="normal"/&gt;
 *     &lt;enumeration value="partition"/&gt;
 *     &lt;enumeration value="persistent-replicate"/&gt;
 *     &lt;enumeration value="preloaded"/&gt;
 *     &lt;enumeration value="replicate"/&gt;
 *     &lt;enumeration value="persistent-partition"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 *
 */
@XmlType(name = "region-attributesData-policy", namespace = "http://geode.apache.org/schema/cache")
@XmlEnum
@Experimental
public enum RegionAttributesDataPolicy implements Serializable {
  @XmlEnumValue("empty")
  EMPTY("empty"),
  @XmlEnumValue("normal")
  NORMAL("normal"),
  @XmlEnumValue("partition")
  PARTITION("partition"),
  @XmlEnumValue("persistent-replicate")
  PERSISTENT_REPLICATE("persistent-replicate"),
  @XmlEnumValue("preloaded")
  PRELOADED("preloaded"),
  @XmlEnumValue("replicate")
  REPLICATE("replicate"),
  @XmlEnumValue("persistent-partition")
  PERSISTENT_PARTITION("persistent-partition");
  private final String value;

  RegionAttributesDataPolicy(String v) {
    value = v;
  }

  public String value() {
    return value;
  }

  public boolean isPersistent() {
    return value.startsWith("persistent");
  }

  public boolean isPartition() {
    return value.endsWith("partition");
  }
}
