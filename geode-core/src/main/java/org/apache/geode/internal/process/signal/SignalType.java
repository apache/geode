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

package org.apache.geode.internal.process.signal;

/**
 * The SignalType class...
 * </p>
 * @since GemFire 7.0
 */
public enum SignalType {
  CONTROL("Other signals that are used by the JVM for control purposes."),
  ERROR("The JVM raises a SIGABRT if it detects a condition from which it cannot recover."),
  EXCEPTION("The operating system synchronously raises an appropriate exception signal whenever an unrecoverable condition occurs."),
  INTERRUPT("Interrupt signals are raised asynchronously, from outside a JVM process, to request shut down."),
  UNKNOWN("Unknown");

  private final String description;

  SignalType(final String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return this.description;
  }

}
