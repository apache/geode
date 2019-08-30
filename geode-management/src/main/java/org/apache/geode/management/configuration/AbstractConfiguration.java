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

package org.apache.geode.management.configuration;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.JsonSerializable;

@Experimental
public abstract class AbstractConfiguration
    implements Identifiable<String>, Serializable, JsonSerializable {
  /**
   * The reserved group name that represents the predefined "cluster" group.
   * Every member of a cluster automatically belongs to this group.
   * Note that this cluster group name is not allowed in some contexts.
   * For example when creating a region, instead of setting the group to CLUSTER,
   * you need to set it to NULL or just let it default.
   */
  public static final String CLUSTER = "cluster";

  /**
   * Returns true if the given "name" represents the predefined "cluster" group.
   * This is true if name is a case-insensitive match for {@link #CLUSTER},
   * is <code>null</code>, or is an empty string.
   */
  public static boolean isCluster(String name) {
    return name == null || name.length() == 0 || name.equalsIgnoreCase(CLUSTER);
  }

  private String group;

  /**
   * this returns a non-null value
   * for cluster level element, it will return "cluster" for sure.
   */
  @JsonIgnore
  public String getConfigGroup() {
    String group = getGroup();
    if (StringUtils.isBlank(group))
      return CLUSTER;
    else
      return group;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }
}
