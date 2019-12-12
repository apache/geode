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
package org.apache.geode.distributed.internal.membership.gms.api;

/**
 * MemberDisconnectedException indicates that we've been kicked out of the cluster.
 * Geode-core generally translates this into a ForcedDisconnectException, which is
 * part of its public API.
 */
public class MemberDisconnectedException extends RuntimeException {
  private static final long serialVersionUID = -3649273301807236514L;

  public MemberDisconnectedException() {}

  public MemberDisconnectedException(String reason) {
    super(reason);
  }

  public MemberDisconnectedException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
