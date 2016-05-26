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

package com.gemstone.gemfire.internal.cache.lru;

public interface LRUClockNode {

  public void setNextLRUNode( LRUClockNode next );
  public void setPrevLRUNode( LRUClockNode prev );
  
  public LRUClockNode nextLRUNode();
  public LRUClockNode prevLRUNode();
  
  /** compute the new entry size and return the delta from the previous entry size */
  public int updateEntrySize(EnableLRU ccHelper);
  /** compute the new entry size and return the delta from the previous entry size
   * @param value then entry's value
   * @since GemFire 6.1.2.9
   */
  public int updateEntrySize(EnableLRU ccHelper, Object value);
  
  public int getEntrySize();
  
  public boolean testRecentlyUsed();
  public void setRecentlyUsed();
  public void unsetRecentlyUsed();

  public void setEvicted();
  public void unsetEvicted();
  public boolean testEvicted();
}
