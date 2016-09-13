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
 * Exception thrown by implementors of {@link TransactionWriter#beforeCommit} to 
 * signal that the current transaction should be aborted.
 * 
 * @see TransactionWriter#beforeCommit
 * @since GemFire 6.5
 *
 */
public class TransactionWriterException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -5557392877576634835L;

  public TransactionWriterException(String s) {
    super(s);
  }
  
  public TransactionWriterException(Throwable t) {
    super(t);
  }
}
