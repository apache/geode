/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.internal.UniqueIdGenerator;

/** MsgId is used to generate unique ids to attach to messages.
 * To get a new id call obtain. When you are done with the id call release.
 * Failure to call release will eventually cause obtain to fail with an exception.
 * <p>Currently ids are in the range 0..32767 inclusive.
 *
 * @author Darrel
 * @since 5.0.2
   
*/
public class MsgIdGenerator {
  /**
   * A value that can be used to indicate that a message does not have an id.
   */
  public static final short NO_MSG_ID = -1;
  private static final short MAX_ID = 32767;
  private static final UniqueIdGenerator uigen = new UniqueIdGenerator(MAX_ID);

  private MsgIdGenerator() {
    // static only; no constructor
  }
  /**
   * Obtains a message id. Callers of this must call release
   * when finished with the id.
   * @throws IllegalStateException if all ids have been obtained
   */
  public static short obtain() {
    return (short)uigen.obtain();
  }
  public static void release(short id) {
    if (id != NO_MSG_ID) {
      uigen.release(id);
    }
  }
}
