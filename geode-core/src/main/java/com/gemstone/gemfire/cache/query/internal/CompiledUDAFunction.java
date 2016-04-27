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

package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.CacheRuntimeException;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Represents the UserDefinedAggregate function node
 * @author ashahid
 * @since 9.0
 *
 */
public class CompiledUDAFunction extends CompiledAggregateFunction {
  private final String udaName;

  public CompiledUDAFunction(CompiledValue expr, int aggFunc, String name) {
    super(expr, aggFunc);
    this.udaName = name;
  }

  @Override
  public Aggregator evaluate(ExecutionContext context) throws FunctionDomainException, TypeMismatchException, NameResolutionException,
                                                      QueryInvocationTargetException {
    Class<Aggregator> aggregatorClass = ((GemFireCacheImpl) context.getCache()).getUDAManager().getUDAClass(this.udaName);
    try {
      return aggregatorClass.newInstance();
    } catch (Exception e) {
      throw new CacheRuntimeException(e) {
      };
    }

  }

  @Override
  public ObjectType getObjectType() {
    return new ObjectTypeImpl(Object.class);
  }

  @Override
  protected String getStringRep() {
    return this.udaName;
  }

}
