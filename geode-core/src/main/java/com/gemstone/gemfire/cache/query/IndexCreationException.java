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
package com.gemstone.gemfire.cache.query;
/**
 * This class is used to represent any partitioned index creation exceptions.
 * 
 */
public class IndexCreationException extends QueryException
{
  private static final long serialVersionUID = -2218359458870240534L;

  /**
   * Constructor with a string message representing the problem.
   * 
   * @param msg message representing the cause of exception
   */
  public IndexCreationException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor with a string message representing the problem and also the 
   * throwable.
   * @param msg representing the cause of exception
   * @param cause the actual exception.
   */
  public IndexCreationException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
