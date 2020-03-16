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
package org.apache.geode.internal.cache;

/**
 * A wrapper class for the operation which has been recovered to be recovered by the htree
 */

class ValueByteWrapper {
  /** stores the bytes that were read * */
  private final byte[] valueBytes;

  private byte userBit;

  /**
   * Constructs the wrapper object
   *
   * @param value byte[] bytes read from oplog
   * @param userBit A byte describing the nature of the value i.e whether it is: serialized ,
   *        invalid or empty byte array etc.
   */

  ValueByteWrapper(byte[] value, byte userBit) {
    this.valueBytes = value;
    this.userBit = userBit;
  }

  /**
   * @return byte[] returns the value bytes stored
   */
  byte[] getValueBytes() {
    return this.valueBytes;
  }

  /**
   * Getter method for the userBit assosciated with the value read from Oplog during initialization
   *
   * @return byte value
   */
  byte getUserBit() {
    return this.userBit;
  }
}
