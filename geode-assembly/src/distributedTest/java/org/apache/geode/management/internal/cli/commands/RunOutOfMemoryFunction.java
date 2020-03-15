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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

public class RunOutOfMemoryFunction implements Function<Void> {

  @Override
  public void execute(FunctionContext<Void> context) {
    byte[] bytes = new byte[Integer.MAX_VALUE / 2];
    byte[] bytes1 = new byte[Integer.MAX_VALUE / 2];
    byte[] bytes2 = new byte[Integer.MAX_VALUE / 2];

    Region<String, byte[]> testRegion = context.getCache().getRegion("/testRegion");
    testRegion.put("byteArray", bytes);
    testRegion.put("byteArray1", bytes1);
    testRegion.put("byteArray2", bytes2);
  }
}
