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
package org.apache.geode.internal.cache.vmotion;

import java.util.Set;

import org.apache.geode.internal.cache.LocalRegion;

/**
 * This class provides 'do-nothing' implementations of all of the methods of interface
 * VMotionObserver.
 */

public class VMotionObserverAdapter implements VMotionObserver {

  /**
   * This callback is called just before CQ registration on the server
   */

  public void vMotionBeforeCQRegistration() {}

  /**
   * This callback is called just before register Interset on the server
   */

  public void vMotionBeforeRegisterInterest() {}

  /**
   * This callback is called before a request for GII is sent.
   */
  public void vMotionDuringGII(Set recipientSet, LocalRegion region) {}
}
