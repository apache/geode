/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.operations;

import java.util.Set;

/**
 * Encapsulates a {@link org.apache.geode.cache.operations.OperationContext.OperationCode#CLOSE_CQ} operation for the pre-operation
 * case.
 * 
 * @since GemFire 5.5
 */
public class CloseCQOperationContext extends ExecuteCQOperationContext {

  /**
   * Constructor for the CLOSE_CQ operation.
   * 
   * @param cqName
   *                the name of the continuous query being closed
   * @param queryString
   *                the query string for this operation
   * @param regionNames
   *                names of regions that are part of the query string
   */
  public CloseCQOperationContext(String cqName, String queryString,
      Set regionNames) {
    super(cqName, queryString, regionNames, false);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.CLOSE_CQ</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.CLOSE_CQ;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return false;
  }

}
