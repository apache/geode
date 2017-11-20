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
package org.apache.geode.connectors.jdbc.internal;

import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PARAMS_DELIMITER;

import java.util.HashMap;
import java.util.Map;

public class ConnectionConfigBuilder {

  private String name;
  private String url;
  private String user;
  private String password;
  private Map<String, String> parameters = new HashMap<>();

  public ConnectionConfigBuilder withName(String name) {
    this.name = name;
    return this;
  }

  public ConnectionConfigBuilder withUrl(String url) {
    this.url = url;
    return this;
  }

  public ConnectionConfigBuilder withUser(String user) {
    this.user = user;
    return this;
  }

  public ConnectionConfigBuilder withPassword(String password) {
    this.password = password;
    return this;
  }

  public ConnectionConfigBuilder withParameters(String[] params) {
    for (String param : params) {
      String[] keyValuePair = param.split(PARAMS_DELIMITER);
      if (keyValuePair.length == 2) {
        parameters.put(keyValuePair[0], keyValuePair[1]);
      }
    }
    return this;
  }

  public ConnectionConfiguration build() {
    return new ConnectionConfiguration(name, url, user, password, parameters);
  }
}
