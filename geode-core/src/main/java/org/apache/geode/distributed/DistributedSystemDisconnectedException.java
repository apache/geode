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
package org.apache.geode.distributed;

import org.apache.geode.CancelException;

/**
 * Thrown when a GemFire distributed system has been terminated.
 * 
 * @since GemFire 6.0
 */

public class DistributedSystemDisconnectedException extends CancelException {

  private static final long serialVersionUID = -2484849299224086250L;

  public DistributedSystemDisconnectedException() {
    super();
  }

  public DistributedSystemDisconnectedException(String message, Throwable cause) {
    super(message, cause);
  }

  public DistributedSystemDisconnectedException(Throwable cause) {
    super(cause);
  }

  public DistributedSystemDisconnectedException(String s) {
    super(s);
  }

}
