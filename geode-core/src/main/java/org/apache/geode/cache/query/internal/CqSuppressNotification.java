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
package org.apache.geode.cache.query.internal;

/**
 * Implements the CqSuppressNotification wrapper.
 *
 * @since Geode 1.14.0
 */
public class CqSuppressNotification {

  private boolean suppressCreate;

  private boolean suppressUpdate;

  private boolean suppressDestroy;

  /**
   * Creates a new CqSuppressNotification.
   */
  public CqSuppressNotification() {
    suppressCreate = false;
    suppressUpdate = false;
    suppressDestroy = false;
  }

  public void setSuppressCreateNotification(boolean suppress) {
    suppressCreate = suppress;
  }

  public void setSuppressUpdateNotification(boolean suppress) {
    suppressUpdate = suppress;
  }

  public void setSuppressDestroyNotification(boolean suppress) {
    suppressDestroy = suppress;
  }

  public int getSuppressNotificationBitMask() {
    int sum = 0;
    if (suppressCreate) {
      sum += 1;
    }
    if (suppressUpdate) {
      sum += 2;
    }
    if (suppressDestroy) {
      sum += 4;
    }
    return sum;
  }

}
