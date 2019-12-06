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

package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.compression.Compressor;

public class TestCompressor1 implements Compressor {
  private static final String prefix = "compressed";
  private static final byte[] prefixBytes = prefix.getBytes();

  @Override
  public byte[] compress(byte[] input) {
    byte[] returnBytes = new byte[prefixBytes.length + input.length];
    System.arraycopy(prefixBytes, 0, returnBytes, 0, prefixBytes.length);
    System.arraycopy(input, 0, returnBytes, prefixBytes.length, input.length);
    return returnBytes;
  }

  @Override
  public byte[] decompress(byte[] input) {
    int numReturnBytes = input.length - prefixBytes.length;
    byte[] returnBytes = new byte[numReturnBytes];
    System.arraycopy(input, prefixBytes.length, returnBytes, 0, numReturnBytes);
    return returnBytes;
  }
}
