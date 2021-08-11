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

import javax.management.ObjectName;

/**
 * This class is used as a key for Notification region Only using ObjectName as key will overwrite
 * entries if put rate for notification is high.
 *
 *
 */
public class NotificationKey implements java.io.Serializable {

  private ObjectName objectName;

  private long currentTime;

  public NotificationKey(ObjectName objectName) {
    this.objectName = objectName;
    this.currentTime = System.nanoTime();
  }

  public ObjectName getObjectName() {
    return objectName;
  }

  public long getCurrentTime() {
    return currentTime;
  }

  public boolean equals(Object anObject) {

    if (this == anObject) {
      return true;
    }
    if (anObject instanceof NotificationKey) {
      NotificationKey anotherFedComp = (NotificationKey) anObject;
      if (anotherFedComp.objectName.equals(this.objectName)
          && anotherFedComp.currentTime == this.currentTime) {
        return true;
      }
    }

    return false;
  }

  public int hashCode() {
    return objectName.hashCode();
  }

}
