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
package org.apache.geode.distributed.internal.deadlock;

import java.io.Serializable;

/**
 * Holds a dependency between one object and another. This class is used as an edge in the @link
 * {@link DependencyGraph}.
 *
 * An example of a dependency is a Thread that is blocking on a lock, or a lock that is held by a
 * thread.
 *
 *
 */
public class Dependency<A, B> implements Serializable {

  private static final long serialVersionUID = 1L;

  public final A depender;
  public final B dependsOn;

  public Dependency(A depender, B dependsOn) {
    this.depender = depender;
    this.dependsOn = dependsOn;
  }

  public A getDepender() {
    return depender;
  }

  public B getDependsOn() {
    return dependsOn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((depender == null) ? 0 : depender.hashCode());
    result = prime * result + ((dependsOn == null) ? 0 : dependsOn.hashCode());
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
    if (!(obj instanceof Dependency)) {
      return false;
    }
    Dependency<?, ?> other = (Dependency<?, ?>) obj;
    if (depender == null) {
      if (other.depender != null) {
        return false;
      }
    } else if (!depender.equals(other.depender)) {
      return false;
    }
    if (dependsOn == null) {
      if (other.dependsOn != null) {
        return false;
      }
    } else if (!dependsOn.equals(other.dependsOn)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return depender + " -> " + dependsOn;
  }

}
