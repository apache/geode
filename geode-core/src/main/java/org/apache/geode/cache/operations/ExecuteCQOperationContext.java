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

package org.apache.geode.cache.operations;

import java.util.Set;

/**
 * Encapsulates a continuous query registeration operation for both the pre-operation and
 * post-operation cases.
 *
 * @since GemFire 5.5
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public class ExecuteCQOperationContext extends QueryOperationContext {

  /** The name of the continuous query being registered. */
  private final String cqName;

  /**
   * Constructor for the EXECUTE_CQ operation.
   *
   * @param cqName the name of the continuous query being registered
   * @param queryString the query string for this operation
   * @param regionNames names of regions that are part of the query string
   * @param postOperation true to set the post-operation flag
   */
  public ExecuteCQOperationContext(String cqName, String queryString, Set regionNames,
      boolean postOperation) {
    super(queryString, regionNames, postOperation);
    this.cqName = cqName;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code> object.
   *
   * @return the <code>OperationCode</code> of this operation
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.EXECUTE_CQ;
  }

  /** Return the name of the continuous query. */
  public String getName() {
    return cqName;
  }

}
