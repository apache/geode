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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.runtime.RuntimeInfo;

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

  public static final String URI_CONTEXT = "/management";
  public static final String URI_VERSION = "/experimental";

  /**
   * Returns the portion of the URI, after {@link #URI_VERSION}, that specifies the type of this
   * instance.
   * It is possible that more than once instance of this type can exist.
   *
   * @return the portion of the URI that identifies the type of this instance
   */
  @JsonIgnore
  public abstract String getEndpoint();

  /**
   * Returns the portion of the URI, after {@link #URI_VERSION}, that specifies the type and id of
   * this instance.
   * This result will uniquely identify a single instance.
   * If the id is null, then null is returned.
   *
   * <p>
   * Note that the result does not include the prefix: <code>http://hostname:port</code>
   * it should return the URI part after /experimental
   *
   * @return {@link #getEndpoint()} + "/" + {@link #getId()}
   */
  @JsonIgnore
  public String getIdentityEndPoint() {
    String id = getId();
    if (StringUtils.isBlank(id))
      return null;
    else {
      String endpoint = getEndpoint();
      if (StringUtils.isBlank(endpoint))
        return null;
      else
        return getEndpoint() + "/" + getId();
    }
  }

  /**
   * Returns the full URI path that uniquely identifies this instance.
   * If the id is null, then null is returned.
   *
   * <p>
   * Note that the result does not include the prefix: <code>http://hostname:port</code>
   *
   * @return {@link #URI_CONTEXT} + {@link #URI_VERSION} + {@link #getIdentityEndPoint()}
   */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public String getUri() {
    if (getIdentityEndPoint() == null) {
      return null;
    }
    return URI_CONTEXT + URI_VERSION + getIdentityEndPoint();
  }

  /**
   * this is to indicate when we need to go gather runtime information for this configuration,
   * should we go to all members in the group, or just any member in the group
   */
  @JsonIgnore
  public boolean isGlobalRuntime() {
    return false;
  }

}
