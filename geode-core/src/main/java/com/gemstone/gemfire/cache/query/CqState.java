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
 * This interface gives information on the state of a CqQuery. 
 * It is provided by the getState method of the CqQuery instance. 
 * 
 * @since GemFire 5.5
 */

public interface CqState {
          
  /**
   * Returns the state in string form.
   */
  public String toString();
  
  /**
   * Returns true if the CQ is in Running state.
   */
  public boolean isRunning();

  /**
   * Returns true if the CQ is in Stopped state.
   */
  public boolean isStopped();
  
  /**
   * Returns true if the CQ is in Closed state.
   */
  public boolean isClosed();
  
  /**
   * Returns true if the CQ is in Closing state.
   */
  public boolean isClosing();
  
}
