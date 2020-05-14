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
package org.apache.geode.test.management;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.Notification;

public class NotificationBuilder implements Serializable {

  private final AtomicReference<String> message = new AtomicReference<>();
  private final AtomicReference<Object> source = new AtomicReference<>();
  private final AtomicReference<String> type = new AtomicReference<>();
  private final AtomicReference<Object> userData = new AtomicReference<>();
  private final AtomicLong sequenceNumber = new AtomicLong();
  private final AtomicLong timeStamp = new AtomicLong();

  public NotificationBuilder message(String message) {
    this.message.set(message);
    return this;
  }

  public NotificationBuilder source(Object source) {
    this.source.set(source);
    return this;
  }

  public NotificationBuilder type(String type) {
    this.type.set(type);
    return this;
  }

  public NotificationBuilder userData(Object userData) {
    this.userData.set(userData);
    return this;
  }

  public NotificationBuilder sequenceNumber(long sequenceNumber) {
    this.sequenceNumber.set(sequenceNumber);
    return this;
  }

  public NotificationBuilder timeStamp(long timeStamp) {
    this.timeStamp.set(timeStamp);
    return this;
  }

  public Notification create() {
    Notification notification = new Notification(type.get(), source.get(), sequenceNumber.get(),
        timeStamp.get(), message.get());
    notification.setUserData(userData.get());
    return notification;
  }
}
