/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author dsmith
 * @since 7.0
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
