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
 * the data it has on disk was never part of the same distributed system as the
 * members that are currently online.
 * 
 * This exception can occur when two members both have persistent files for the
 * same region, but they were online at different times, so the contents of their
 * persistent files are completely different. In that case, gemfire throws this
 * exception rather than discarding one of the sets of persistent files.
 * 
 * @author dsmith
 * @since 6.5
 */
public class ConflictingPersistentDataException extends GemFireException {

  private static final long serialVersionUID = -2629287782021455875L;

  public ConflictingPersistentDataException() {
    super();
  }

  public ConflictingPersistentDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConflictingPersistentDataException(String message) {
    super(message);
  }

  public ConflictingPersistentDataException(Throwable cause) {
    super(cause);
  }

  

}
