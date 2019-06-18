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
package org.apache.geode.management.api;

import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;

/**
 * a thing that can have a group. if it has no group, then it belongs to the default group CLUSTER
 * See also MultiGroupCacheElement for things that can have more than one group
 */
@Experimental
public interface Groupable {
  String CLUSTER = "cluster";

  String getGroup();

  void setGroup(String group);

  /**
   * this returns a non-null value
   * for cluster level element, it will return "cluster" for sure.
   */
  @XmlTransient
  @JsonIgnore
  default String getConfigGroup() {
    String group = getGroup();
    if (StringUtils.isBlank(group)) {
      return CLUSTER;
    } else {
      return group;
    }
  }
}
