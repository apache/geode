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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

/**
 * this keeps all HATEOAS links related to a particular configuration object.
 * only the map (links) is serialized back to the client, nothing get de-serialized.
 *
 * <p>
 * <strong>Implementation Note: Serializable Interface</strong>
 * </p>
 * <p>
 * This class implements {@link Serializable} because it is used as a field in
 * {@link org.apache.geode.management.api.ClusterManagementResult}, which must be serializable
 * for DUnit distributed testing. When ClusterManagementResult objects are passed between JVMs
 * in DUnit tests, all fields in the object graph must also be serializable.
 * </p>
 *
 * <p>
 * <strong>Serialization Safety:</strong>
 * </p>
 * <ul>
 * <li>All fields are inherently serializable: String primitives and HashMap (which is
 * Serializable)</li>
 * <li>The links field uses HashMap&lt;String, String&gt; which is fully serializable</li>
 * <li>No transient fields or complex objects that could break serialization</li>
 * </ul>
 *
 * <p>
 * This change enables ClusterManagementResult to be fully serializable for cross-JVM
 * communication in DUnit tests without affecting production functionality.
 * </p>
 */
public class Links implements Serializable {
  /**
   * Serial version UID for serialization compatibility.
   * Required for Serializable interface to maintain compatibility across code changes.
   */
  private static final long serialVersionUID = 1L;
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
   *
   * @param key the key at which to add the URL
   * @param url the URL to be added
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
