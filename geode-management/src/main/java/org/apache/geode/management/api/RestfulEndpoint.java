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


import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;

/**
 * a restful service request just needs:
 * 1. an endpoint to list all instances (e.g. "/regions")
 * 2. an id mechanism to distinguish instance (e.g. "{id}")
 * 3. an endpoint to update or delete an individual instance (e.g. "/regions/{id}")
 */
@Experimental
public interface RestfulEndpoint extends Identifiable<String> {
  String URI_CONTEXT = "/management";
  String URI_VERSION = "/experimental";

  /**
   * Returns the portion of the URI, after {@link #URI_VERSION}, that specifies the type of this
   * instance.
   * It is possible that more than once instance of this type can exist.
   *
   * @return the portion of the URI that identifies the type of this instance
   */
  @JsonIgnore
  String getEndpoint();

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
  default String getIdentityEndpoint() {
    String id = getId();
    if (StringUtils.isBlank(id)) {
      return null;
    } else {
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
   * @return {@link #URI_CONTEXT} + {@link #URI_VERSION} + {@link #getIdentityEndpoint()}
   */
  @JsonIgnore()
  default String getUri() {
    if (getIdentityEndpoint() == null) {
      return null;
    }
    return URI_CONTEXT + URI_VERSION + getIdentityEndpoint();
  }
}
