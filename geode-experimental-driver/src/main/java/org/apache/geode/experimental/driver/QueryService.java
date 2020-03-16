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
package org.apache.geode.experimental.driver;

/**
 * This interface abstracts the creation of query abstractions.
 */
public interface QueryService {
  /**
   * Creates a new object that allows the execution of querys in the distributed system.
   *
   * @param queryString OQL query.
   * @param <T> Type of result returned from the query. May differ from the key and values types
   *        of any involved regions.
   * @return Query abstraction.
   */
  <T> Query<T> newQuery(String queryString);
}
