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
package org.apache.geode.cache.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Consists a map of index names and Exceptions thrown during index creation using
 * {@link QueryService#createDefinedIndexes()}. An {@link Index} could throw one of the following
 * exceptions:
 * <ul>
 * <li>{@link IndexNameConflictException}</li>
 * <li>{@link IndexExistsException}</li>
 * <li>{@link IndexInvalidException}</li>
 * <li>{@link UnsupportedOperationException}</li>
 * </ul>
 *
 * @since GemFire 8.1
 *
 */
public class MultiIndexCreationException extends Exception {
  private static final long serialVersionUID = 6312081720315894780L;
  /**
   * Map of indexName -> Exception
   */
  private final Map<String, Exception> exceptionsMap;

  /**
   * Creates an {@link MultiIndexCreationException}
   *
   */
  public MultiIndexCreationException(HashMap<String, Exception> exceptionMap) {
    super();
    exceptionsMap = exceptionMap;
  }

  /**
   * Returns a map of index names and Exceptions
   *
   * @return a map of index names and Exceptions
   */
  public Map<String, Exception> getExceptionsMap() {
    return exceptionsMap;
  }

  /**
   * Returns a set of names for the indexes that failed to create
   *
   * @return set of failed index names
   */
  public Set<String> getFailedIndexNames() {
    return exceptionsMap.keySet();
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (Map.Entry<String, Exception> entry : exceptionsMap.entrySet()) {
      sb.append("Creation of index: ").append(entry.getKey()).append(" failed due to: ")
          .append(entry.getValue()).append(", ");
    }
    sb.delete(sb.length() - 2, sb.length());
    return sb.toString();
  }

}
