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

package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.GemFireCheckedException;

import java.util.ArrayList;
import java.util.List;
/**
 * An exception thrown during batch processing.
 *
 *
 * @since 7.0
 */
// Note that since this class is inside of an internal package,
// we make it extend Exception, thereby making it a checked exception.
public class BatchException70 extends GemFireCheckedException {
private static final long serialVersionUID = -6707074107791305564L;

  protected int index;
  private int batchId;
  
  List<BatchException70> exceptions;
  
  /**
   * Required for serialization
   * @param l 
   */
  public BatchException70(List<BatchException70> l) {
    super(l.get(0).getMessage());
    this.batchId = l.get(0).getBatchId();
    this.exceptions = new ArrayList<BatchException70>();
    this.exceptions.addAll(l);
    this.index = this.exceptions.get(0).getIndex();
  }
  
  public  BatchException70(String msg, int batchId) {
    super(msg);
    this.batchId = batchId;
  }

  public BatchException70(String msg, int index, int id) {
    super(msg);
    this.index = index;
    this.batchId = id;
  }
  
  /**
   * Constructor.
   * Creates an instance of <code>RegionQueueException</code> with the
   * specified cause.
   * @param cause the causal Throwable
   * @param index the index in the batch list where the exception occurred
   */
  public BatchException70(Throwable cause, int index) {
    super(cause);
    this.index = index;
  }

  
  public BatchException70(Throwable cause, int index, int id) {
    super(cause);
    this.index = index;
    this.batchId = id;
  }
  
  public BatchException70(String message, Throwable cause, int index, int id) {
	super(message, cause);
	this.index = index;
	this.batchId = id;
  }
  
  /**
   * Answers the index in the batch where the exception occurred
   * @return the index in the batch where the exception occurred
   */
  public int getIndex() {
    return this.index;
  }

  /**
   * @return the batchId
   */
  public int getBatchId() {
    return batchId;
  }
  
  public List<BatchException70> getExceptions() {
    return this.exceptions;
  }
  
}
