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

import java.io.IOException;
import java.util.List;

/**
 * This interface represents from the client-side the execution of a query on the server-side.
 *
 * @param <T> Type of result returned from the query. May differ from the key and value types
 *        of any involved regions.
 */
public interface Query<T> {
  /**
   * Executes this query with the specified parameters.
   *
   * @param bindParameters Optional list of bind parameters.
   * @return Possibly empty list of results from the query.
   * @throws IOException If an error occurred communicating with the distributed system.
   */
  List<T> execute(Object... bindParameters) throws IOException;
}
