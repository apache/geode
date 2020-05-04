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

import java.io.Serializable;
import java.util.Objects;

import javax.management.ObjectName;

/**
 * This class is used as a key for Notification region Only using ObjectName as key will overwrite
 * entries if put rate for notification is high.
 */
public class NotificationKey implements Serializable {

  private final ObjectName objectName;
  private final long currentTime;

  public NotificationKey(ObjectName objectName) {
    this.objectName = objectName;
    currentTime = System.nanoTime();
  }

  public ObjectName getObjectName() {
    return objectName;
  }

  public long getCurrentTime() {
    return currentTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NotificationKey other = (NotificationKey) o;
    return currentTime == other.currentTime &&
        Objects.equals(objectName, other.objectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectName, currentTime);
  }

  @Override
  public String toString() {
    return "NotificationKey{" +
        "objectName=" + objectName +
        ", currentTime=" + currentTime +
        '}';
  }

  private static final long serialVersionUID = 6382673938605954446L;
}
