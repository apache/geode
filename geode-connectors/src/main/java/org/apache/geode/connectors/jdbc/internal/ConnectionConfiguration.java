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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConnectionConfiguration implements Serializable {
  private static final Object USER = "user";
  private static final Object PASSWORD = "password";

  private final String name;
  private final String url;
  private final String user;
  private final String password;
  private final Map<String, String> parameters;

  public ConnectionConfiguration(String name, String url, String user, String password,
      Map<String, String> parameters) {
    this.name = name;
    this.url = url;
    this.user = user;
    this.password = password;
    this.parameters =
        parameters == null ? new HashMap<>() : Collections.unmodifiableMap(parameters);
  }

  public String getName() {
    return name;
  }

  public String getUrl() {
    return url;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public Properties getConnectionProperties() {
    Properties properties = new Properties();

    properties.putAll(parameters);
    if (user != null) {
      properties.put(USER, user);
    }
    if (password != null) {
      properties.put(PASSWORD, password);
    }
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectionConfiguration that = (ConnectionConfiguration) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (url != null ? !url.equals(that.url) : that.url != null) {
      return false;
    }
    if (user != null ? !user.equals(that.user) : that.user != null) {
      return false;
    }
    if (password != null ? !password.equals(that.password) : that.password != null) {
      return false;
    }
    return parameters != null ? parameters.equals(that.parameters) : that.parameters == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (url != null ? url.hashCode() : 0);
    result = 31 * result + (user != null ? user.hashCode() : 0);
    result = 31 * result + (password != null ? password.hashCode() : 0);
    result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ConnectionConfiguration{" + "name='" + name + '\'' + ", url='" + url + '\'' + ", user='"
        + user + '\'' + ", password='" + password + '\'' + ", parameters=" + parameters + '}';
  }
}
