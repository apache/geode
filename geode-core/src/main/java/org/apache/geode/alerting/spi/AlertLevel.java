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

import org.apache.geode.logging.spi.LogWriterLevel;

/**
 * Defines the {@code LogWriterLevel}s that are available for use as a threshold for generating and
 * listening for system {@code Alert}s.
 */
public enum AlertLevel {

  WARNING(LogWriterLevel.WARNING.intLevel()),
  ERROR(LogWriterLevel.ERROR.intLevel()),
  SEVERE(LogWriterLevel.SEVERE.intLevel()),
  NONE(LogWriterLevel.NONE.intLevel());

  public static AlertLevel find(int intLevel) {
    for (AlertLevel alertLevel : values()) {
      if (alertLevel.intLevel == intLevel) {
        return alertLevel;
      }
    }
    throw new IllegalArgumentException("No AlertLevel found for intLevel " + intLevel);
  }

  private final int intLevel;

  AlertLevel(int intLevel) {
    this.intLevel = intLevel;
  }

  public int intLevel() {
    return intLevel;
  }

  public boolean meetsOrExceeds(AlertLevel alertLevel) {
    return intLevel() < alertLevel.intLevel();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "." + name() + "(" + intLevel + ")";
  }
}
