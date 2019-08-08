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
package org.apache.geode.alerting.spi;

/**
 * Executes an action that is protected against generating additional {@code Alert}s. Even if the
 * executed action generates log statements that meet the configured {@code AlertLevel}, no
 * {@code Alert} will be raised.
 */
public class AlertingAction {

  private static final ThreadLocal<Boolean> ALERTING = ThreadLocal.withInitial(() -> Boolean.FALSE);

  public static void execute(final Runnable action) {
    ALERTING.set(true);
    try {
      action.run();
    } finally {
      ALERTING.set(false);
    }
  }

  public static boolean isThreadAlerting() {
    return ALERTING.get();
  }
}
