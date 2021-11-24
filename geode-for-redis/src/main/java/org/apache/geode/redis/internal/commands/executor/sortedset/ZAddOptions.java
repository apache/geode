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
 *
 */

package org.apache.geode.redis.internal.commands.executor.sortedset;


import org.apache.geode.redis.internal.commands.executor.BaseSetOptions;


/**
 * Class representing different options that can be used with Redis Sorted Set ZADD command.
 */
public class ZAddOptions extends BaseSetOptions {
  private final boolean isCH;
  private final boolean isINCR;

  public ZAddOptions(Exists existsOption, boolean isCH, boolean isINCR) {
    super(existsOption);

    this.isCH = isCH;
    this.isINCR = isINCR;
  }

  public boolean isCH() {
    return isCH;
  }

  public boolean isINCR() {
    return isINCR;
  }

}
