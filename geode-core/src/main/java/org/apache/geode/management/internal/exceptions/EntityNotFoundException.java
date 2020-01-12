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

package org.apache.geode.management.internal.exceptions;

import org.apache.geode.GemFireException;

public class EntityNotFoundException extends GemFireException {

  private boolean statusOK;

  public EntityNotFoundException() {}

  public EntityNotFoundException(String message) {
    super(message);
  }

  public EntityNotFoundException(String message, boolean statusOK) {
    super(message);
    this.statusOK = statusOK;
  }

  public EntityNotFoundException(Throwable cause) {
    super(cause);
  }

  public EntityNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public boolean isStatusOK() {
    return statusOK;
  }
}
