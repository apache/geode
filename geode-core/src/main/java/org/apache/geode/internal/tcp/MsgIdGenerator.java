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
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.internal.UniqueIdGenerator;

/** MsgId is used to generate unique ids to attach to messages.
 * To get a new id call obtain. When you are done with the id call release.
 * Failure to call release will eventually cause obtain to fail with an exception.
 * <p>Currently ids are in the range 0..32767 inclusive.
 *
 * @since GemFire 5.0.2
   
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
