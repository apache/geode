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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.api.Links;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * This class represents either a configuration or a filter.
 * <p>
 * As a configuration of a managed entity it can be used to
 * either {@linkplain ClusterManagementService#create(AbstractConfiguration) create},
 * {@linkplain ClusterManagementService#update(AbstractConfiguration) update},
 * or {@linkplain ClusterManagementService#delete(AbstractConfiguration) delete} it,
 * or to describe it in the result of
 * {@linkplain ClusterManagementService#list(AbstractConfiguration) list}
 * or {@linkplain ClusterManagementService#get(AbstractConfiguration) get}.
 * <p>
 * As a filter as input to {@linkplain ClusterManagementService#list(AbstractConfiguration) list}
 * or {@linkplain ClusterManagementService#get(AbstractConfiguration) get}
 * it can be used to specify what entities to return.
 *
 * @param <R> the RuntimeInfo that corresponds to this configuration. Each non-abstract subclass
 *        needs to supply this when it extends this class.
 */
@Experimental
public abstract class AbstractConfiguration<R extends RuntimeInfo>
    implements Identifiable<String>, JsonSerializable {

  /**
   * The reserved group name that represents the predefined "cluster" group.
   * Every member of a cluster automatically belongs to this group.
   * Note that this cluster group name is not allowed in some contexts.
   * For example when creating a region, instead of setting the group to CLUSTER,
   * you need to set it to NULL or just let it default.
   */
  public static final String CLUSTER = "cluster";

  /**
   * Returns true if the given "groupName" represents the predefined "cluster" group.
   * This is true if "groupName" is a case-insensitive match for {@link #CLUSTER},
   * is <code>null</code>, or is an empty string.
   */
  public static boolean isCluster(String groupName) {
    return isBlank(groupName) || groupName.equalsIgnoreCase(CLUSTER);
  }

  public static String getGroupName(String group) {
    return isCluster(group) ? CLUSTER : group;
  }

  @JsonIgnore
  public String getGroup() {
    return null;
  }

  /**
   * Returns the portion of the URI that uniquely, in the scope of {@link #getEndpoint()},
   * identifies this instance.
   * Some implementations of this class require that some other attribute be set
   * (for example {@link Region#setName(String)}) before this method will return
   * a non-null value. Refer to the javadocs on subclasses for details.
   */
  @Override
  public abstract String getId();

  @JsonIgnore
  public abstract Links getLinks();

  /**
   * Returns true if the RuntimeInfo will be the same on all members;
   * false if each member can have different RuntimeInfo.
   */
  @JsonIgnore
  public boolean isGlobalRuntime() {
    return false;
  }
}
