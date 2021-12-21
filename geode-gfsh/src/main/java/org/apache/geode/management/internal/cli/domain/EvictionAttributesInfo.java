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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.management.internal.cli.util.RegionAttributesDefault;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;

public class EvictionAttributesInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  private String evictionAction = "";
  private String evictionAlgorithm = "";
  private int evictionMaxValue = 0;
  private Map<String, String> nonDefaultAttributes;

  public EvictionAttributesInfo(EvictionAttributes ea) {
    EvictionAction evictAction = ea.getAction();

    if (evictAction != null) {
      evictionAction = evictAction.toString();
    }
    EvictionAlgorithm evictionAlgo = ea.getAlgorithm();
    if (evictionAlgo != null) {
      evictionAlgorithm = evictionAlgo.toString();
    }
    evictionMaxValue = ea.getMaximum();
  }

  public String getEvictionAction() {
    return evictionAction;
  }

  public String getEvictionAlgorithm() {
    return evictionAlgorithm;
  }

  public int getEvictionMaxValue() {
    return evictionMaxValue;
  }

  public boolean equals(Object obj) {
    if (obj instanceof EvictionAttributesInfo) {
      EvictionAttributesInfo their = (EvictionAttributesInfo) obj;
      return evictionAction.equals(their.getEvictionAction())
          && evictionAlgorithm.equals(their.getEvictionAlgorithm())
          && evictionMaxValue == their.getEvictionMaxValue();
    } else {
      return false;
    }
  }

  public int hashCode() {
    return 42; // any arbitrary constant will do

  }

  public Map<String, String> getNonDefaultAttributes() {
    if (nonDefaultAttributes == null) {
      nonDefaultAttributes = new HashMap<String, String>();
    }

    if (evictionMaxValue != RegionAttributesDefault.EVICTION_MAX_VALUE) {
      nonDefaultAttributes.put(RegionAttributesNames.EVICTION_MAX_VALUE,
          Long.toString(evictionMaxValue));
    }
    if (evictionAction != null
        && !evictionAction.equals(RegionAttributesDefault.EVICTION_ACTION)) {
      nonDefaultAttributes.put(RegionAttributesNames.EVICTION_ACTION, evictionAction);
    }
    if (evictionAlgorithm != null
        && !evictionAlgorithm.equals(RegionAttributesDefault.EVICTION_ALGORITHM)) {
      nonDefaultAttributes.put(RegionAttributesNames.EVICTION_ALGORITHM, evictionAlgorithm);
    }
    return nonDefaultAttributes;
  }
}
