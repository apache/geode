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

package org.apache.geode.management.configuration;

/**
 * Public ENUM to indicate type of Index on a Geode {@link Region} used by the Management API.
 * @see Index
 *
 * @since Geode 1.12
 */
public enum IndexType {
  FUNCTIONAL("RANGE"),
  @Deprecated
  HASH_DEPRECATED("HASH"),
  PRIMARY_KEY("KEY");

  private String name;

  IndexType(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public static IndexType valueOfSynonym(String name) {
    name = name.toUpperCase();
    switch(name) {
      case "KEY":
        return valueOf("PRIMARY_KEY");
      case "RANGE":
        return valueOf("FUNCTIONAL");
      case "HASH":
        return valueOf("HASH_DEPRECATED");
    }
    return valueOf(name);
  }
}
