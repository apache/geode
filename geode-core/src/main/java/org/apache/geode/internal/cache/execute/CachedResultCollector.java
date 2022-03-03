/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.execute;

import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;


/**
 * This interface is intended to be implemented by those ResultCollector classes whose
 * getResult() methods are delegated to another class and the real result computation
 * is implemented in the getResultInternal() methods.
 *
 * This is useful in conjunction with the ResultCollectorHolder class whose getResult()
 * methods make sure that the result is only computed once.
 *
 */
public interface CachedResultCollector<T, S> extends ResultCollector<T, S> {

  Object getResultInternal() throws FunctionException;

  Object getResultInternal(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException;
}
