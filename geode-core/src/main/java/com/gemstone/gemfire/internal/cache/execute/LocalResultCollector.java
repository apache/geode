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

package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;

/**
 * Extends {@link ResultCollector} interface to provide for methods that are
 * required internally by the product.
 * 
 */
public interface LocalResultCollector<T, S>
    extends ResultCollector<T, S> {

  /** set any exception during execution of a Function in the collector */
  void setException(Throwable exception);

  /** keep a reference of processor, if required, to avoid it getting GCed */
  void setProcessor(ReplyProcessor21 processor);

  /**
   * get the {@link ReplyProcessor21}, if any, set in the collector by a
   * previous call to {@link #setProcessor(ReplyProcessor21)}
   */
  ReplyProcessor21 getProcessor();
}
