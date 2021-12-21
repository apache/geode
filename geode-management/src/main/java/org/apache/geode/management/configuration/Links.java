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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

/**
 * this keeps all HATEOAS links related to a particular configuration object.
 * only the map (links) is serialized back to the client, nothing get de-serialized.
 */
public class Links {
  public static final String HREF_PREFIX = "#HREF";
  public static final String URI_CONTEXT = "/management";
  public static final String URI_VERSION = "/v1";
  private String self;
  private String list;
  private final Map<String, String> links;

  public Links() {
    links = new HashMap<>();
  }

  public Links(String id, String listUri) {
    links = new HashMap<>();
    setList(listUri);

    if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(listUri)) {
      setSelf(list + "/" + id);
    }
  }

  @JsonIgnore
  public String getSelf() {
    return self;
  }

  @JsonIgnore
  public void setSelf(String self) {
    this.self = self;
    addLink("self", self);
  }

  @JsonIgnore
  public String getList() {
    return list;
  }

  @JsonIgnore
  public void setList(String list) {
    this.list = list;
    addLink("list", list);
  }

  /**
   * adds the additional HATEOAS links
   */
  public void addLink(String key, String url) {
    links.put(key, qualifyUrl(url));
  }

  // this is just to make sure nothing get de-serialized
  @JsonAnySetter
  public void anySetter(String key, String url) {}

  @JsonAnyGetter
  public Map<String, String> getLinks() {
    return links;
  }

  private static String qualifyUrl(String uri) {
    if (uri.startsWith(URI_VERSION)) {
      return HREF_PREFIX + URI_CONTEXT + uri;
    }
    if (uri.startsWith(URI_CONTEXT)) {
      return HREF_PREFIX + uri;
    }
    if (uri.startsWith(HREF_PREFIX)) {
      return uri;
    }
    return HREF_PREFIX + URI_CONTEXT + URI_VERSION + uri;
  }
}
