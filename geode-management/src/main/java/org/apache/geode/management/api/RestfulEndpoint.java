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

import org.apache.geode.lang.Identifiable;

/**
 * established the minimum requirements for a restful service request -- must have a unique id, a
 * scope, an endpoint to list all of its kind, an endpoint to list itself, and a type declaration
 * for the return type when list or other operations are performed
 */

// "type parameter is never used" warning is suppressed, because actually it is used to create
// the linkage between request and response type in the signature of ClusterManagementService.list
@SuppressWarnings("unused")
public interface RestfulEndpoint<R extends RuntimeResponse> extends Identifiable<String> {
  /** should return null if no group */
  String getGroup();

  /** should return "cluster" if no group */
  String getConfigGroup();

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
   * this needs to return the uri portion after the /geode-management/v2 that points to a single
   * entity. If the id is not available for the object, this will return null
   *
   * @return e.g. /regions/regionA
   */
  String getUri();
}
