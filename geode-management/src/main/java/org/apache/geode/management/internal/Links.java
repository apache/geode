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
package org.apache.geode.management.internal;

import static org.apache.geode.management.api.RestfulEndpoint.URI_CONTEXT;
import static org.apache.geode.management.api.RestfulEndpoint.URI_VERSION;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.management.api.RestfulEndpoint;

/**
 * HATEOAS data-structures
 */
public class Links {
  public static final String SELF = "self";
  public static final String API_ROOT = "api root";
  public static final String SWAGGER = "swagger";
  public static final String DOCS = "docs";
  public static final String WIKI = "wiki";

  public static LinkedHashMap<String, String> singleItem(RestfulEndpoint configuration) {
    LinkedHashMap<String, String> ret = new LinkedHashMap<>();
    String singleItemUri = configuration.getUri();
    if (singleItemUri != null) {
      ret.put(SELF, singleItemUri);
    }
    return ret;
  }

  public static void addApiRoot(Map<String, String> links) {
    links.putIfAbsent(API_ROOT, URI_CONTEXT + URI_VERSION + "/");
  }

  /**
   * Enriches a {@link LinkedHashMap} with URI links to documentation that assists in the
   * understanding of Geode management API
   *
   * The keys in the map are {@link #SWAGGER}, {@link #DOCS} and {@link #WIKI}
   */
  public static LinkedHashMap<String, String> rootLinks() {
    LinkedHashMap<String, String> ret = new LinkedHashMap<>();
    ret.put(SWAGGER, URI_CONTEXT + "/swagger-ui.html");
    ret.put(DOCS, "https://geode.apache.org/docs");
    ret.put(WIKI, "https://cwiki.apache.org/confluence/display/GEODE/Management+REST+API");
    return ret;
  }
}
