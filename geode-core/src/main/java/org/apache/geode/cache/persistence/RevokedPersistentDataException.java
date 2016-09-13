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
package com.gemstone.gemfire.cache.persistence;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.admin.AdminDistributedSystem;

/**
 * Thrown when a member with persistence is recovering, and it discovers that
 * other members in the system have revoked the persistent data stored on this
 * member.
 * 
 * This exception can also occur if set of persistent files was thought to be
 * lost and was revoked, but is later brought online. Once a persistent member
 * is revoked, that member cannot rejoin the distributed system unless the
 * persistent files are removed. See
 * {@link AdminDistributedSystem#revokePersistentMember(java.net.InetAddress, String)}
 * 
 * @since GemFire 7.0
 */
public class RevokedPersistentDataException extends GemFireException {

  private static final long serialVersionUID = 0L;

  public RevokedPersistentDataException() {
    super();
  }

  public RevokedPersistentDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public RevokedPersistentDataException(String message) {
    super(message);
  }

  public RevokedPersistentDataException(Throwable cause) {
    super(cause);
  }

  

}
