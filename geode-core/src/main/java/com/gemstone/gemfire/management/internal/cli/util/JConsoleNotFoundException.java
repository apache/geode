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

package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.GemFireException;

/**
 * The JConsoleNotFoundException class is a RuntimeException class that indicates that the JDK JConsole tool could
 * not be located in the file system.
 * </p>
 * @see com.gemstone.gemfire.GemFireException
 * @since 7.0
 */
@SuppressWarnings("unused")
public class JConsoleNotFoundException extends GemFireException {

  public JConsoleNotFoundException() {
  }

  public JConsoleNotFoundException(final String message) {
    super(message);
  }
  public JConsoleNotFoundException(final Throwable cause) {
    super(cause);
  }

  public JConsoleNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
