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
package org.apache.geode.internal.concurrent;

import java.io.Serializable;

/**
 * These methods are the same as on the jdk 1.6 java.lang.management.LockInfo, but this class is
 * serializable.
 *
 * Retained for backwards compatibility reasons.
 *
 */
public class LI implements Serializable {

  private static final long serialVersionUID = -6014738350371493969L;

  public final String className;
  public final int identityHashCode;
  private StackTraceElement lockedStackFrame;

  public LI(String className, int identityHashCode) {
    this.className = className;
    this.identityHashCode = identityHashCode;
  }

  public LI(String className, int identityHashCode, StackTraceElement lockedStackFrame) {
    this.lockedStackFrame = lockedStackFrame;
    this.className = className;
    this.identityHashCode = identityHashCode;
  }

  public String getClassName() {
    return className;
  }

  public int getIdentityHashCode() {
    return identityHashCode;
  }

  // This comes from monitor info. It will null if the lock is
  // not a monitor.
  public StackTraceElement getLockedStackFrame() {
    return lockedStackFrame;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((className == null) ? 0 : className.hashCode());
    result = prime * result + identityHashCode;
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
    if (!(obj instanceof LI)) {
      return false;
    }
    LI other = (LI) obj;
    if (className == null) {
      if (other.className != null) {
        return false;
      }
    } else if (!className.equals(other.className)) {
      return false;
    }
    if (identityHashCode != other.identityHashCode) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return className + '@' + Integer.toHexString(identityHashCode);
  }
}
