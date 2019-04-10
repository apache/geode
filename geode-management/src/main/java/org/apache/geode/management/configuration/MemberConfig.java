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

import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.RestfulEndpoint;

@Experimental
public class MemberConfig extends CacheElement implements RestfulEndpoint {

  private static final long serialVersionUID = -6262538068604902018L;

  public static final String MEMBER_CONFIG_ENDPOINT = "/members";

  private boolean isLocator;
  private boolean isCoordinator;
  private String id;
  private String host;
  private String pid;
  private List<Integer> ports;

  public MemberConfig() {

  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isLocator() {
    return isLocator;
  }

  public void setLocator(boolean locator) {
    isLocator = locator;
  }

  public boolean isCoordinator() {
    return isCoordinator;
  }

  public void setCoordinator(boolean coordinator) {
    isCoordinator = coordinator;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public List<Integer> getPorts() {
    return ports;
  }

  public void setPorts(List<Integer> port) {
    this.ports = port;
  }

  @Override
  public String getEndpoint() {
    return MEMBER_CONFIG_ENDPOINT;
  }

  @Override
  public String getId() {
    return id;
  }
}
