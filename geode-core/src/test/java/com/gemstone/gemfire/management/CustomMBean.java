/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management;

import javax.management.AttributeChangeNotification;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
/**
 * A simple MBean to test various aspects of federation
 *
 */
public class CustomMBean extends NotificationBroadcasterSupport implements
    CustomMXBean {

  private long systemTime;
  private String name;

  public CustomMBean(String name) {
    this.name = name;
  }

  @Override
  public long getSystemTime() {
    return System.currentTimeMillis();
  }

  @Override
  public void setSystemTime(long systemTIme) {
    this.systemTime = systemTime;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String fetchName() {
    // TODO Auto-generated method stub
    return name;
  }

  @Override
  public void writeName(String name) {
    this.name = name;

    Notification n = new AttributeChangeNotification(this, sequenceNumber++,
        System.currentTimeMillis(), "staticField changed", "staticField",
        "int", name, this.name);

    sendNotification(n);
  }

  private long sequenceNumber = 1;

}
