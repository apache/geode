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
package org.apache.geode.cache.util;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;

/**
 * This partition resolver requires every key of the partitioned region to be an instance of String
 * and to contain at least one "|" delimiter. The prefix, the substring of the key that precedes the
 * first delimiter, is returned by getRoutingObject.
 * 
 * @since Geode 1.2.0
 */
public class StringPrefixPartitionResolver implements PartitionResolver<String, Object> {

  /**
   * The default delimiter is "|". Currently this class only uses the default delimiter but in a
   * future release configuring the delimiter may be supported.
   */
  public static final String DEFAULT_DELIMITER = "|";

  /**
   * Creates a prefix resolver with the default delimiter.
   */
  public StringPrefixPartitionResolver() {}

  /**
   * Returns the prefix of the String key that precedes the first "|" in the key.
   * 
   * @throws ClassCastException if the key is not an instance of String
   * @throws IllegalArgumentException if the key does not contain at least one "|".
   */
  @Override
  public Object getRoutingObject(EntryOperation<String, Object> opDetails) {
    String key = opDetails.getKey();
    String delimiter = getDelimiter();
    int idx = key.indexOf(delimiter);
    if (idx == -1) {
      throw new IllegalArgumentException(
          "The key \"" + key + "\" does not contains the \"" + delimiter + "\" delimiter.");
    }
    return key.substring(0, idx);
  }

  @Override
  public java.lang.String getName() {
    return getClass().getName();
  }

  private java.lang.String getDelimiter() {
    return DEFAULT_DELIMITER;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StringPrefixPartitionResolver)) {
      return false;
    }
    StringPrefixPartitionResolver other = (StringPrefixPartitionResolver) o;
    return other.getName().equals(getName()) && other.getDelimiter().equals(getDelimiter());
  }

  @Override
  public void close() {}
}
