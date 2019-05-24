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
import java.util.List;

import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.geode.lang.Identifiable;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public interface CacheElement extends Identifiable<String>, Serializable {
  static <T extends Identifiable> boolean exists(List<T> list, String id) {
    return list.stream().anyMatch(o -> o.getId().equals(id));
  }

  static <T extends Identifiable> T findElement(List<T> list, String id) {
    return list.stream().filter(o -> o.getId().equals(id)).findFirst().orElse(null);
  }

  static <T extends Identifiable> void removeElement(List<T> list, String id) {
    list.removeIf(t -> t.getId().equals(id));
  }

  /**
   * this returns a non-null value
   * for cluster level element, it will return "cluster" for sure.
   */
  @XmlTransient
  @JsonIgnore
  String getConfigGroup();

  /**
   * this returns the first group set by the user
   * if no group is set, this returns null
   */
  @XmlTransient
  String getGroup();

  @JsonSetter
  void setGroup(String group);
}
