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
 * The MemberNotFoundException is a GemFirException indicating that a member by name could not be found in the GemFire
 * distributed system.
 * </p>
 * @see com.gemstone.gemfire.GemFireException
 * @since GemFire 7.0
 */
// TODO this GemFireException should be moved to a more appropriate package!
@SuppressWarnings("unused")
public class MemberNotFoundException extends GemFireException {

  public MemberNotFoundException() {
  }

  public MemberNotFoundException(final String message) {
    super(message);
  }

  public MemberNotFoundException(final Throwable cause) {
    super(cause);
  }

  public MemberNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
