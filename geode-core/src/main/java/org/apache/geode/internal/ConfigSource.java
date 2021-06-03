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
  };

  private final Type type;
  private final String description;

  private ConfigSource(Type t) {
    this.type = t;
    switch (t) {
      case API:
        this.description = "api";
        break;
      case SYSTEM_PROPERTY:
        this.description = "system property";
        break;
      case FILE:
        this.description = "file";
        break;
      case SECURE_FILE:
        this.description = "secure file";
        break;
      case XML:
        this.description = "cache.xml";
        break;
      case RUNTIME:
        this.description = "runtime modification";
        break;
      case LAUNCHER:
        this.description = "launcher";
        break;
      default:
        this.description = "";
    }
  }

  private ConfigSource(String fileName, boolean secure) {
    if (secure) {
      this.type = Type.SECURE_FILE;
      this.description = (fileName == null) ? "secure file" : fileName;
    } else {
      this.type = Type.FILE;
      this.description = (fileName == null) ? "file" : fileName;
    }
  }

  /**
   * @return returns the type of this source
   */
  public Type getType() {
    return this.type;
  }

  public String getDescription() {
    return this.description;
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
    return this.description;
  }
}
