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
package org.apache.geode.protocol.protobuf;

public enum ProtocolErrorCode {
  GENERIC_FAILURE(1000),
  VALUE_ENCODING_ERROR(1100),
  UNSUPPORTED_VERSION(1101),
  AUTHENTICATION_FAILED(1200),
  AUTHORIZATION_FAILED(1201),
  UNAUTHORIZED_REQUEST(1202),
  LOW_MEMORY(1300),
  DATA_UNREACHABLE(1301),
  CONSTRAINT_VIOLATION(2000),
  BAD_QUERY(2001),
  REGION_NOT_FOUND(2100),
  QUERY_PARAMETER_MISMATCH(2200),
  QUERY_BIND_FAILURE(2201),
  QUERY_NOT_PERMITTED(2202),
  QUERY_TIMEOUT(2203);

  ProtocolErrorCode(int value) {
    codeValue = value;
  }

  public int codeValue;
}
