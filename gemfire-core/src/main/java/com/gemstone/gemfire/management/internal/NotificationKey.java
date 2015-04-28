/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import javax.management.ObjectName;

/**
 * This class is used as a key for Notification region Only using ObjectName as
 * key will overwrite entries if put rate for notification is high.
 * 
 * @author rishim
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
          && anotherFedComp.currentTime == this.currentTime)
        return true;
    }

    return false;
  }

  public int hashCode() {
    return objectName.hashCode();
  }

}
