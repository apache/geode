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
package com.gemstone.gemfire.cache;

/**
 * Indicates that an attempt was made to transactionally modify multiple keys that
 * are not colocated on the same data host.
 * This can be thrown while doing transactional operations or during commit.
 *
 * <p>This exception only occurs when a transaction
 * is hosted on a member that is not
 * the initiator of the transaction.
 *
 * <p>Note: a rebalance can cause this exception to be thrown for data that
 * is usually colocated. This is because data can be moved from one node to another
 * during the time between the original transactional operations and the commit. 
 *
 * @since GemFire 6.5
 */
public class TransactionDataNotColocatedException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public TransactionDataNotColocatedException(String s) {
    super(s);
  }

  public TransactionDataNotColocatedException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
