/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Javadoc stub for legacy Tomcat class org.apache.catalina.ha.session.SerializablePrincipal.
 * Only the minimal surface required by DeltaSession is implemented. This stub exists
 * exclusively for aggregate Javadoc generation and MUST NOT be on any runtime classpath.
 */
package org.apache.catalina.ha.session;

import java.io.Serializable;
import java.security.Principal;

import org.apache.catalina.Realm;
import org.apache.catalina.realm.GenericPrincipal;

@SuppressWarnings({"unused"})
public class SerializablePrincipal implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String name;

  public SerializablePrincipal(String name) {
    this.name = name;
  }

  public static SerializablePrincipal createPrincipal(GenericPrincipal gp) {
    return new SerializablePrincipal(gp == null ? null : gp.getName());
  }

  public Principal getPrincipal(Realm realm) {
    // Provide a minimal Principal; GenericPrincipal construction not reproduced.
    return new Principal() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String toString() {
        return name;
      }
    };
  }

  @Override
  public String toString() {
    return name;
  }
}
