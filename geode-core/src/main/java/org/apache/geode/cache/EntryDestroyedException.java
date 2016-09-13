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
package org.apache.geode.cache;
/**
 * Indicates that a method was invoked on an entry that has been destroyed.
 *
 *
 * 
 * @see Region.Entry
 * @since GemFire 3.0
 */
public class EntryDestroyedException extends CacheRuntimeException
{
  private static final long serialVersionUID = 831865939772672542L;
  /** Constructs a new <code>EntryDestroyedException</code>. */ 
  public EntryDestroyedException()
  {
     super();
  }

  /**
   * Constructs a new <code>EntryDestroyedException</code> with the message.
   * @param s the detailed message for this exception
   */
  public EntryDestroyedException(String s)
  {
    super(s);
  }

  /** Constructs a new <code>EntryDestroyedException</code> with a detailed message
   * and a causal exception.
   * @param s the message
   * @param ex a causal Throwable
   */
  public EntryDestroyedException(String s, Throwable ex)
  {
    super(s, ex);
  }
  
  /** Construct a <code>EntryDestroyedException</code> with a cause.
   * @param ex the causal Throwable
   */  
  public EntryDestroyedException(Throwable ex) {
    super(ex);
  }
}
