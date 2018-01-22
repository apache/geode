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
package org.apache.geode.internal.protocol;

public enum ProtocolErrorCode {
  AUTHENTICATION_REQUIRED(10),
  AUTHENTICATION_FAILED(11),
  ALREADY_AUTHENTICATED(12),
  AUTHENTICATION_NOT_SUPPORTED(13),
  AUTHORIZATION_FAILED(20),
  INVALID_REQUEST(50),
  SERVER_ERROR(100),
  NO_AVAILABLE_SERVER(101);

  ProtocolErrorCode(int value) {
    codeValue = value;
  }

  public int codeValue;
}
