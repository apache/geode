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
package org.apache.geode.management;

/**
 * This interface acts as UserData section of JMX notifications of type "system.alert".
 * It contains some additional information apart from the Notification details.
 * 
 * @since GemFire  8.0
 */
public interface JMXNotificationUserData { 
  /**
   * The level at which this alert is issued.
   */
  public static final String ALERT_LEVEL = "AlertLevel"; 
  
  /**
   * The member of the distributed system that issued the alert, or
   * null if the issuer is no longer a member of the distributed system.
   * This constant is defined in org.apache.geode.management.UserData
   */
  
  public static final String MEMBER = "Member"; 
  
  /** 
   * The thread causing the alert
   */
  
  public static final String THREAD = "Thread";

}
