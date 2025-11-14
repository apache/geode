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
package org.apache.catalina.ha.session;

import java.io.Serializable;
import java.security.Principal;
import java.util.Arrays;
import java.util.List;

import org.apache.catalina.Realm;
import org.apache.catalina.realm.GenericPrincipal;

/**
 * Serializable wrapper for GenericPrincipal.
 * This class replaces the legacy Tomcat SerializablePrincipal which was removed in recent versions.
 * It provides a way to serialize and deserialize Principal objects for session replication.
 */
public class SerializablePrincipal implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String name;
  private final String password;
  private final List<String> roles;

  private SerializablePrincipal(String name, String password, List<String> roles) {
    this.name = name;
    this.password = password;
    this.roles = roles;
  }

  /**
   * Create a SerializablePrincipal from a GenericPrincipal
   */
  public static SerializablePrincipal createPrincipal(GenericPrincipal principal) {
    if (principal == null) {
      return null;
    }
    // Note: GenericPrincipal.getPassword() is deprecated and removed in Tomcat 10+
    // We store null for password as it's not needed for session replication
    return new SerializablePrincipal(
        principal.getName(),
        null, // password not stored for security
        Arrays.asList(principal.getRoles()));
  }

  /**
   * Reconstruct a GenericPrincipal from this SerializablePrincipal
   */
  public Principal getPrincipal(Realm realm) {
    // Tomcat 9 constructor: GenericPrincipal(String name, String password, List<String> roles)
    return new GenericPrincipal(name, password, roles);
  }

  @Override
  public String toString() {
    return "SerializablePrincipal[name=" + name + ", roles=" + roles + "]";
  }
}
