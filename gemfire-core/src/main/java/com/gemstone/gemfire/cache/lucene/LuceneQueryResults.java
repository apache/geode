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
package com.gemstone.gemfire.cache.lucene;

import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * <p>
 * Defines the interface for a container of lucene query result collected from function
 * execution.<br>
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */

public interface LuceneQueryResults<E> {

  /* get next page of result if pagesize is specified in query, otherwise, return null */
  public List<E> getNextPage();
  
  /* Is next page of result available */
  public boolean hasNextPage();
  
  /* total number of items */
  public int size();

}
