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

package com.gemstone.gemfire.internal.util;

/**
 * The SunAPINotFoundException class is a RuntimeException indicating that the Sun API classes and components could
 * not be found, which is most likely the case when we are not running a Sun JVM (like HotSpot).
 * </p>
 * @see java.lang.RuntimeException
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class SunAPINotFoundException extends RuntimeException {

  public SunAPINotFoundException() {
  }

  public SunAPINotFoundException(final String message) {
    super(message);
  }

  public SunAPINotFoundException(final Throwable t) {
    super(t);
  }

  public SunAPINotFoundException(final String message, final Throwable t) {
    super(message, t);
  }

}
