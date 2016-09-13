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

package com.gemstone.gemfire.cache.server;

/**
 * Used to configure queuing on a cache server for client subscriptions.
 * <p>
 * <UL>
 * <LI>
 * For eviction-policy <b>none</b> client queue entries are not evicted to disk <br></LI>
 * <LI>For eviction-policy <b>mem</b> client queue entries are evicted to disk when limit is
 * reached, defined by <b>capacity</b></LI>
 * <LI>For eviction-policy <b>entry</b> HA entries are evicted to disk when limit is
 * reached, defined by <b>capacity</b></LI>
 * </UL>
 * <br/>
 * 
 * The capacity limits the total amount of memory or entries for all client queues
 * on held on this server. If this server hosts multiple client queues, they will
 * all share the same capacity.
 * 
 * <p>
 * <b>Configuration: </b>
 * <p>
 * The <code>client queue</code> is configurable declaratively or
 * programmatically. Declarative configuration is achieved through defining the
 * configuration parameters in a <code>cache.xml</code> file. Programmatic
 * configuration may be achieved by first instantiating a
 * <code>CacheServer</code> object and get {@link CacheServer#getClientSubscriptionConfig} 
 * <code>ClientSubscriptionConfig</code> object and modify each desired parameter and value.
 * <p>
 * <p>
 * 
 * If you are using a <code>cache.xml</code> file to create a
 * <code>CacheServer</code> declaratively, you can do the following to configure
 * <code>ClientSubscriptionConfig</code> and to have <b>none</b> eviction policy 
 * no need to specify client-subscription tag as it is a default one.
 *</p>
 *
 *<pre>
 *<code>
 * &lt;cache-server port=4444&gt;
 *   &lt;client-subscription eviction-policy=&quot;entry | mem&quot; capacity=35 overflow-directory=&quot;OverflowDir&quot;&gt;&lt;/client-subscription&gt;
 * &lt;/cache-server&gt;
 * </code>
 *</pre>
 * @see #getEvictionPolicy
 * @see #getCapacity
 * 
 * 
 * @since GemFire 5.7
 */

public interface ClientSubscriptionConfig {
  
  /**
   * The default limit that is assigned to client subscription.
   */
  public static final int DEFAULT_CAPACITY = 1;
  
  /**
   * The default eviction policy that is assigned to client subscription.
   */
  public static final String DEFAULT_EVICTION_POLICY = "none";
  
  /**
   * The default overflow directory that is assigned to client subscription.
   */
  public static final String DEFAULT_OVERFLOW_DIRECTORY = ".";
  
  /**
   * Returns the capacity of the client queue.
   * will be in MB for eviction-policy <b>mem</b> else
   * number of entries
   * @see #DEFAULT_CAPACITY
   * @since GemFire 5.7
   */
  public int getCapacity();

  /**
   * Sets the capacity of the client queue.
   * will be in MB for eviction-policy <b>mem</b> else
   * number of entries
   * @see #DEFAULT_CAPACITY
   * @since GemFire 5.7
   */
  public void setCapacity(int capacity);

  /**
   * Returns the eviction policy that is executed when capacity of the client queue is reached.
   * @see #DEFAULT_EVICTION_POLICY
   * @since GemFire 5.7
   */
  public String getEvictionPolicy();

  /**
   * Sets the eviction policy that is executed when capacity of the client queue is reached.
   * @see #DEFAULT_EVICTION_POLICY
   * @since GemFire 5.7
   */
  public void setEvictionPolicy(String policy);

  /**
   * Sets the overflow directory for a client queue 
   * @param overflowDirectory the overflow directory for a client queue's overflowed entries
   * @since GemFire 5.7
   * @deprecated as of 6.5 use {@link #setDiskStoreName(String)} instead
   */
  public void setOverflowDirectory(String overflowDirectory);

  /**
   * Answers the overflow directory for a client queue's
   * overflowed client queue entries.
   * @return the overflow directory for a client queue's
   * overflowed entries
   * @since GemFire 5.7
   * @deprecated as of 6.5 use {@link #getDiskStoreName} instead
   */
  public String getOverflowDirectory();
  /**
   * Sets the disk store name for overflow  
   * @param diskStoreName 
   * @since GemFire 6.5
   */
  public void setDiskStoreName(String diskStoreName);

  /**
   * get the diskStoreName for overflow
   * @since GemFire 6.5
   */
  public String getDiskStoreName();
}
