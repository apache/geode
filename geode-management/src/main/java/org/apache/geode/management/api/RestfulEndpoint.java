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
import com.fasterxml.jackson.annotation.JsonProperty;
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
   * this returns the URI that display the list of entries.
   * it should return URI the part after /experimental
   *
   * @return e.g. /regions
   */
  @JsonIgnore
  String getEndpoint();

  /**
   * return the uri that points to a single entity. If the id is not available for the object,
   * this will return null
   *
   * it should return the URI part after /experimental
   *
   * @return e.g. /regions/regionA
   */
  @JsonIgnore
  default String getIdentityEndPoint() {
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
   * return the full uri path that points to a single entity. If the id is not available for the
   * object, this will return null
   *
   * it should return the URI part after http://hostname:port
   *
   * @return e.g. /management/experimental/regions/regionA
   */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  default String getUri() {
    if (getIdentityEndPoint() == null) {
      return null;
    }
    return URI_CONTEXT + URI_VERSION + getIdentityEndPoint();
  }
}
