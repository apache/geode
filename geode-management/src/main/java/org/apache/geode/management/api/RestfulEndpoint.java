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
import org.apache.geode.lang.Identifiable;

/**
 * a restful service request just needs:
 * 1. an endpoint to list all instances (e.g. "/regions")
 * 2. an id mechanism to distinguish instance (e.g. "{id}")
 * 3. an endpoint to update or delete an individual instance (e.g. "/regions/{id}")
 */
@Experimental
public interface RestfulEndpoint extends Identifiable<String> {
  /**
   * this needs to return the uri portion after the /geode-management/v2 that points to the
   * list of entities
   *
   * @return e.g. /regions
   */
  @JsonIgnore
  // @ApiModelProperty(hidden = true)
  String getEndpoint();

  /**
   * return the uri portion after the /geode-management/v2 that points to a single
   * entity. If the id is not available for the object, this will return null
   *
   * @return e.g. /regions/regionA
   */
  @XmlTransient
  default String getUri() {
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
}
