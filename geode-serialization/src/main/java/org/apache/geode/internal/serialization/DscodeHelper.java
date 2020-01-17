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
package org.apache.geode.internal.serialization;

import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.annotations.Immutable;

/**
 * DscodeHelper is used by advanced serialization code to convert between bytes and
 * DSCODEs.
 */
public class DscodeHelper {

  @Immutable
  private static final DSCODE[] dscodes = new DSCODE[128];

  static {
    Arrays.stream(DSCODE.values()).filter(dscode -> dscode.toByte() >= 0)
        .forEach(dscode -> dscodes[dscode.toByte()] = dscode);
  }

  public static DSCODE toDSCODE(final byte value) throws IOException {
    try {
      DSCODE result = dscodes[value];
      if (result == null) {
        throw new IOException("Unknown header byte " + value);
      }
      return result;
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IOException("Unknown header byte: " + value);
    }
  }
}
