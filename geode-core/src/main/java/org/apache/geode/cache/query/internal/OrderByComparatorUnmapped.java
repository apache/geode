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
package org.apache.geode.cache.query.internal;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.types.ObjectType;

@Deprecated
public class OrderByComparatorUnmapped extends OrderByComparator {

  private final Map<Object, Object[]> orderByMap;

  public OrderByComparatorUnmapped(List<CompiledSortCriterion> orderByAttrs,
      ObjectType objType, ExecutionContext context) {
    super(orderByAttrs, objType, context);
    if (objType.isStructType()) {
      orderByMap = new Object2ObjectOpenCustomHashMap<Object, Object[]>(
          new StructBag.ObjectArrayFUHashingStrategy());
    } else {
      this.orderByMap = new HashMap<Object, Object[]>();
    }

  }

  @Override
  void addEvaluatedSortCriteria(Object row, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    this.orderByMap.put(row, this.calculateSortCriteria(context, row));
  }

  @Override
  protected Object[] evaluateSortCriteria(Object row) {
    return (Object[]) orderByMap.get(row);
  }


  private Object[] calculateSortCriteria(ExecutionContext context, Object row)

  throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    CompiledSortCriterion csc;
    if (orderByAttrs != null) {
      Object[] evaluatedResult = new Object[this.orderByAttrs.size()];

      Iterator<CompiledSortCriterion> orderiter = orderByAttrs.iterator();
      int index = 0;
      while (orderiter.hasNext()) {
        csc = orderiter.next();
        Object[] arr = new Object[2];
        if (csc.getColumnIndex() == -1) {
          arr[0] = csc.evaluate(context);
        } else {
          arr[0] = csc.evaluate(row, context);
        }
        arr[1] = Boolean.valueOf(csc.getCriterion());
        evaluatedResult[index++] = arr;
      }
      return evaluatedResult;
    }
    return null;
  }

}
