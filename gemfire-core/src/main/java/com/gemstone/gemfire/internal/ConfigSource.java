/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.Serializable;

/**
 * Describes where the value of a configuration attribute came from.
 * 
 * @author darrel
 * @since 7.0
 */
public class ConfigSource implements Serializable {
  private static final long serialVersionUID = -4097017272431018553L;
  public enum Type {API, SYSTEM_PROPERTY, FILE, SECURE_FILE, XML, RUNTIME, LAUNCHER};
  private final Type type;
  private final String description;
  
  private ConfigSource(Type t, String d) {
    this.type = t;
    this.description = d;
  }
  /**
   * @return returns the type of this source
   */
  public Type getType() {
    return this.type;
  }
  public String getDescription() {
    String result = this.description;
    if (result == null) {
      switch (getType()) {
      case API: result = "api"; break;
      case SYSTEM_PROPERTY: result = "system property"; break;
      case FILE: result = "file"; break;
      case SECURE_FILE: result = "secure file"; break;
      case XML: result = "cache.xml"; break;
      case RUNTIME: result = "runtime modification"; break;
      case LAUNCHER: result = "launcher"; break;
      }
    }
    return result;
  }

  private static final ConfigSource API_SINGLETON = new ConfigSource(Type.API, null);
  private static final ConfigSource SYSPROP_SINGLETON = new ConfigSource(Type.SYSTEM_PROPERTY, null);
  private static final ConfigSource XML_SINGLETON = new ConfigSource(Type.XML, null);
  private static final ConfigSource RUNTIME_SINGLETON = new ConfigSource(Type.RUNTIME, null);
  private static final ConfigSource LAUNCHER_SINGLETON = new ConfigSource(Type.LAUNCHER, null);
  
  public static ConfigSource api() {return API_SINGLETON;}
  public static ConfigSource sysprop() {return SYSPROP_SINGLETON;}
  public static ConfigSource xml() {return XML_SINGLETON;}
  public static ConfigSource runtime() {return RUNTIME_SINGLETON;}
  public static ConfigSource file(String fileName, boolean secure) {return new ConfigSource(secure ? Type.SECURE_FILE : Type.FILE, fileName);}
  public static ConfigSource launcher() {return LAUNCHER_SINGLETON;}
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ConfigSource other = (ConfigSource) obj;
    if (description == null) {
      if (other.description != null)
        return false;
    } else if (!description.equals(other.description))
      return false;
    if (type != other.type)
      return false;
    return true;
  }
  
  @Override
  public String toString() {
    return this.description;
  }
}
