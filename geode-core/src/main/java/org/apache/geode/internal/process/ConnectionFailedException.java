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
package org.apache.geode.internal.process;

/**
 * A ConnectionFailedException is thrown if connection to a process fails.
 * 
 * @since GemFire 7.0
 */
public final class ConnectionFailedException extends Exception {
  private static final long serialVersionUID = 5622636452836752700L;

  /**
   * Creates a new <code>ConnectionFailedException</code>.
   */
  public ConnectionFailedException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>ConnectionFailedException</code> that was
   * caused by a given exception
   */
  public ConnectionFailedException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>ConnectionFailedException</code> that was
   * caused by a given exception
   */
  public ConnectionFailedException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
