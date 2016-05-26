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

package com.gemstone.gemfire.distributed.internal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An atomic integer with update methods that check to see if the value is equal
 * to a special flag. Care should be taken to ensure that the value can never
 * become the special value accidentally. For example, a long that can never go
 * negative with normal use could have a terminal state of Long.MIN_VALUE
 * 
 * @since GemFire 6.0
 */
public class AtomicLongWithTerminalState extends AtomicLong {
  
  private static final long serialVersionUID = -6130409343386576390L;
  
  

  public AtomicLongWithTerminalState() {
    super();
  }



  public AtomicLongWithTerminalState(long initialValue) {
    super(initialValue);
  }

  /**
   * Add and the the given delta to the long, unless the long
   * has been set to the terminal state.
   * @param terminalState
   * @param delta
   * @return the new value of the field, or the terminalState if the field
   * is already set to the terminal state.
   */
  public long compareAddAndGet(long terminalState, long delta) {
    while(true) {
      long current = get();
      if(current == terminalState) {
        return terminalState;
      }
      long newValue = current +delta;
      if (compareAndSet(current, newValue)) {
        return newValue;
      }
    }
  }
}
