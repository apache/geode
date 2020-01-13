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
package org.apache.geode.distributed.internal.membership.api;

/**
 * MembershipClosedException is thrown if membership services are no longer
 * available. This exception may be thrown by any membership API and does
 * not appear in API interfaces.
 */
public class MembershipClosedException extends RuntimeException {
  private static final long serialVersionUID = 6112938405434046127L;

  public MembershipClosedException() {}

  public MembershipClosedException(String reason) {
    super(reason);
  }

  public MembershipClosedException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
