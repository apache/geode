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
package org.apache.geode.internal.cache.tier.sockets;

public class ConflationDUnitTestHelper {

  /**
   * set the boolean for starting the dispatcher thread a bit later.
   *
   */
  public static void setIsSlowStart() {
    setIsSlowStart("5000");
  }


  /**
   * Set the boolean to make the dispatcher thread pause <code>milis</code> miliseconds.
   *
   */
  public static void setIsSlowStart(String milis) {
    CacheClientProxy.isSlowStartForTesting = true;
    System.setProperty("slowStartTimeForTesting", milis);
  }

  /**
   * Unset the boolean to start the dispatcher thread.
   *
   */
  public static void unsetIsSlowStart() {
    CacheClientProxy.isSlowStartForTesting = false;
  }

}
