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
package org.apache.geode.internal;

import java.io.Serializable;

import org.apache.geode.annotations.Immutable;

/**
 * Describes where the value of a configuration attribute came from.
 *
 * @since GemFire 7.0
 */
@Immutable
public class ConfigSource implements Serializable {
  private static final long serialVersionUID = -4097017272431018553L;

  public enum Type {
    API, SYSTEM_PROPERTY, FILE, SECURE_FILE, XML, RUNTIME, LAUNCHER
  }

  private final Type type;
  private final String description;

  private ConfigSource(Type t) {
    type = t;
    switch (t) {
      case API:
        description = "api";
        break;
      case SYSTEM_PROPERTY:
        description = "system property";
        break;
      case FILE:
        description = "file";
        break;
      case SECURE_FILE:
        description = "secure file";
        break;
      case XML:
        description = "cache.xml";
        break;
      case RUNTIME:
        description = "runtime modification";
        break;
      case LAUNCHER:
        description = "launcher";
        break;
      default:
        description = "";
    }
  }

  private ConfigSource(String fileName, boolean secure) {
    if (secure) {
      type = Type.SECURE_FILE;
      description = (fileName == null) ? "secure file" : fileName;
    } else {
      type = Type.FILE;
      description = (fileName == null) ? "file" : fileName;
    }
  }

  /**
   * @return returns the type of this source
   */
  public Type getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  @Immutable
  private static final ConfigSource API_SINGLETON = new ConfigSource(Type.API);
  @Immutable
  private static final ConfigSource SYSPROP_SINGLETON = new ConfigSource(Type.SYSTEM_PROPERTY);
  @Immutable
  private static final ConfigSource XML_SINGLETON = new ConfigSource(Type.XML);
  @Immutable
  private static final ConfigSource RUNTIME_SINGLETON = new ConfigSource(Type.RUNTIME);
  @Immutable
  private static final ConfigSource LAUNCHER_SINGLETON = new ConfigSource(Type.LAUNCHER);

  public static ConfigSource api() {
    return API_SINGLETON;
  }

  public static ConfigSource sysprop() {
    return SYSPROP_SINGLETON;
  }

  public static ConfigSource xml() {
    return XML_SINGLETON;
  }

  public static ConfigSource runtime() {
    return RUNTIME_SINGLETON;
  }

  public static ConfigSource file(String fileName, boolean secure) {
    return new ConfigSource(fileName, secure);
  }

  public static ConfigSource launcher() {
    return LAUNCHER_SINGLETON;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ConfigSource other = (ConfigSource) obj;

    return (type == other.type && description.equals(other.description));
  }

  @Override
  public String toString() {
    return description;
  }
}
