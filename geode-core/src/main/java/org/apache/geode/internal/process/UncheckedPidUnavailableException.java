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
package org.apache.geode.internal.process;

/**
 * Wraps an {@link PidUnavailableException} with an unchecked exception.
 */
public class UncheckedPidUnavailableException extends RuntimeException {
  private static final long serialVersionUID = -4896120572252598765L;

  /**
   * Creates a new {@code UncheckedPidUnavailableException}.
   */
  public UncheckedPidUnavailableException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@code UncheckedPidUnavailableException} that was caused by a given exception
   */
  public UncheckedPidUnavailableException(final String message,
      final PidUnavailableException cause) {
    super(message, cause);
  }

  /**
   * Creates a new {@code UncheckedPidUnavailableException} that was caused by a given exception
   */
  public UncheckedPidUnavailableException(final PidUnavailableException cause) {
    super(cause.getMessage(), cause);
  }

  /**
   * Returns the cause of this exception.
   *
   * @return the {@code PidUnavailableException} which is the cause of this exception.
   */
  @Override
  public PidUnavailableException getCause() {
    return (PidUnavailableException) super.getCause();
  }
}
