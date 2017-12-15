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
package org.apache.geode.internal.protocol.protobuf.v1;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ErrorCodesDUnitTest {
  @Test
  public void testProtobufErrorCodesMatchProtocolErrorCodes() {
    BasicTypes.ErrorCode[] protobufErrorCodes = BasicTypes.ErrorCode.values();
    ProtocolErrorCode[] protocolErrorCodes = ProtocolErrorCode.values();

    // These arrays should identical except the protobuf is required to have a 0 value, and defines
    // an UNRECOGNIZED entry
    assertEquals(protobufErrorCodes.length - 2, protocolErrorCodes.length);
    for (int i = 0; i < protobufErrorCodes.length; ++i) {
      if (i == 0) {
        assertEquals(BasicTypes.ErrorCode.INVALID_ERROR_CODE, protobufErrorCodes[i]);
      } else if (i == protobufErrorCodes.length - 1) {
        assertEquals(BasicTypes.ErrorCode.UNRECOGNIZED, protobufErrorCodes[i]);
      } else {
        assertEquals(protobufErrorCodes[i].getNumber(), protocolErrorCodes[i - 1].codeValue);
      }
    }
  }
}
