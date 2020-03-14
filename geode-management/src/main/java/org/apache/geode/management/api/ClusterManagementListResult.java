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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 *
 * @param <T> the type of the static config, e.g. RegionConfig
 * @param <R> the type of the corresponding runtime information, e.g. RuntimeRegionInfo
 */
@Experimental
public class ClusterManagementListResult<T extends AbstractConfiguration<R>, R extends RuntimeInfo>
    extends ClusterManagementResult {

  public ClusterManagementListResult() {}

  public ClusterManagementListResult(StatusCode statusCode, String message) {
    super(statusCode, message);
  }

  private final Map<String, EntityInfo<T, R>> entities = new HashMap<>();

  /**
   * Returns the combined payload of the list call
   */
  @JsonIgnore
  public List<EntityGroupInfo<T, R>> getEntityGroupInfo() {
    return entities.values().stream()
        .flatMap(x -> x.getGroups().stream())
        .collect(Collectors.toList());
  }

  // this annotation makes sure we always show the result even though it's an empty list
  @JsonInclude
  public List<EntityInfo<T, R>> getResult() {
    return new ArrayList<>(entities.values());
  }

  public void setResult(List<EntityInfo<T, R>> entities) {
    this.entities.clear();
    for (EntityInfo<T, R> entity : entities) {
      this.entities.put(entity.getId(), entity);
    }
  }

  public void addEntityInfo(EntityInfo<T, R> entityInfo) {
    entities.put(entityInfo.getId(), entityInfo);
  }

  @JsonIgnore
  public void setEntityGroupInfo(List<EntityGroupInfo<T, R>> entityGroupInfos) {
    entities.clear();
    for (EntityGroupInfo<T, R> entityGroupInfo : entityGroupInfos) {
      String id = entityGroupInfo.getConfiguration().getId();
      EntityInfo<T, R> entity = entities.get(id);
      if (entity == null) {
        entity = new EntityInfo<>();
        entity.setId(id);
        entities.put(id, entity);
      }
      entity.getGroups().add(entityGroupInfo);
    }
  }

  /**
   * Returns only the static config portion of the results
   */
  @JsonIgnore
  public List<T> getConfigResult() {
    return getEntityGroupInfo().stream().map(EntityGroupInfo::getConfiguration)
        .collect(Collectors.toList());
  }

  /**
   * Returns only the runtime information portion of the results
   */
  @JsonIgnore
  public List<R> getRuntimeResult() {
    return getEntityGroupInfo().stream().flatMap(r -> r.getRuntimeInfo().stream())
        .collect(Collectors.toList());
  }
}
