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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import org.apache.commons.lang3.StringUtils;

public class Links {
  public static final String URI_CONTEXT = "/management";
  public static final String URI_VERSION = "/experimental";

  private String self;
  private String list;
  private Map<String, String> others;

  public Links() {
    others = new HashMap<>();
  }

  public Links(String id, String listUri) {
    others = new HashMap<>();
    this.list = listUri;

    if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(listUri)) {
      this.self = listUri + "/" + id;
    }
  }

  public String getSelf() {
    return self;
  }

  public void setSelf(String self) {
    this.self = self;
  }

  public String getList() {
    return list;
  }

  public void setList(String list) {
    this.list = list;
  }

  @JsonAnySetter
  public void addLink(String key, String url) {
    others.put(key, url);
  }

  @JsonAnyGetter
  public Map<String, String> getOthers() {
    return others;
  }
}
