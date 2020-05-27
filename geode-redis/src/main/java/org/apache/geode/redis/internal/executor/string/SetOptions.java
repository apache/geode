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

package org.apache.geode.redis.internal.executor.string;

import java.io.Serializable;

/**
 * Class representing different options that can be used with Redis string SET command.
 */
public class SetOptions implements Serializable {

  private final Exists exists;
  private final long expirationMillis;
  private final boolean keepTTL;

  public SetOptions(Exists exists, long expiration, boolean keepTTL) {
    this.exists = exists;
    this.expirationMillis = expiration;
    this.keepTTL = keepTTL;
  }

  public boolean isNX() {
    return exists.equals(Exists.NX);
  }

  public boolean isXX() {
    return exists.equals(Exists.XX);
  }

  public Exists getExists() {
    return exists;
  }

  public long getExpiration() {
    return expirationMillis;
  }

  public boolean isKeepTTL() {
    return keepTTL;
  }

  public enum Exists {
    NONE,

    /**
     * Only set if key does not exist
     */
    NX,

    /**
     * Only set if key already exists
     */
    XX;
  }
}
