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
package org.apache.geode.cache.query.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.pdx.internal.PdxString;

public class OrderByComparatorMapped extends OrderByComparator {

  private final Map<Object, Object[]> orderByMap;

  public OrderByComparatorMapped(List<CompiledSortCriterion> orderByAttrs, ObjectType objType,
      ExecutionContext context) {
    super(orderByAttrs, objType, context);
    if (objType.isStructType()) {
      orderByMap = new Object2ObjectOpenCustomHashMap<Object, Object[]>(
          new StructBag.ObjectArrayFUHashingStrategy());
    } else {
      orderByMap = new HashMap<Object, Object[]>();
    }
  }

  @Override
  void addEvaluatedSortCriteria(Object row, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    orderByMap.put(row, calculateSortCriteria(context, row));
  }

  @Override
  public int evaluateSortCriteria(Object obj1, Object obj2) {
    int result = -1;
    Object[] list1 = evaluateSortCriteria(obj1);
    Object[] list2 = evaluateSortCriteria(obj2);
    if (list1.length != list2.length) {
      Support.assertionFailed("Error Occurred due to improper sort criteria evaluation ");
    } else {
      for (int i = 0; i < list1.length; i++) {
        Object[] arr1 = (Object[]) list1[i];
        Object[] arr2 = (Object[]) list2[i];

        if (arr1[0] == null) {
          result = (arr2[0] == null ? 0 : -1);
        } else if (arr2[0] == null) {
          result = 1;
        } else if (arr1[0] == QueryService.UNDEFINED) {
          result = (arr2[0] == QueryService.UNDEFINED ? 0 : -1);
        } else if (arr2[0] == QueryService.UNDEFINED) {
          result = 1;
        } else {
          if (arr1[0] instanceof Number && arr2[0] instanceof Number) {
            Number num1 = (Number) arr1[0];
            Number num2 = (Number) arr2[0];
            result = Double.compare(num1.doubleValue(), num2.doubleValue());
          } else {
            if (arr1[0] instanceof PdxString && arr2[0] instanceof String) {
              arr2[0] = new PdxString((String) arr2[0]);
            } else if (arr2[0] instanceof PdxString && arr1[0] instanceof String) {
              arr1[0] = new PdxString((String) arr1[0]);
            }
            result = ((Comparable) arr1[0]).compareTo(arr2[0]);
          }
        }

        if (result != 0) {
          // not equal, change the sign based on the order by type (asc,
          // desc)
          if (((Boolean) arr1[1]).booleanValue()) {
            result *= -1;
          }
          break;
        }
      }
    }
    return result;
  }

  @Override
  protected Object[] evaluateSortCriteria(Object row) {
    return orderByMap.get(row);
  }

  private Object[] calculateSortCriteria(ExecutionContext context, Object row)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (orderByAttrs != null) {
      Object[] evaluatedResult = new Object[orderByAttrs.size()];
      int index = 0;
      for (CompiledSortCriterion csc : orderByAttrs) {
        Object[] arr = new Object[2];
        if (csc.getColumnIndex() == -1) {
          arr[0] = csc.evaluate(context);
        } else {
          arr[0] = csc.evaluate(row, context);
        }
        arr[1] = csc.getCriterion();
        evaluatedResult[index++] = arr;
      }
      return evaluatedResult;
    }
    return null;
  }
}
