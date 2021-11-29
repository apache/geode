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

import java.util.Comparator;
import java.util.List;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.pdx.internal.PdxString;

/**
 * A generic comparator class which compares two Object/StructImpl according to their sort criteria
 * specified in order by clause.
 */
public class OrderByComparator implements Comparator {
  private final ObjectType objType;
  private final ExecutionContext context;
  protected final List<CompiledSortCriterion> orderByAttrs;

  public OrderByComparator(List<CompiledSortCriterion> orderByAttrs, ObjectType objType,
      ExecutionContext context) {
    this.objType = objType;
    this.context = context;
    this.orderByAttrs = orderByAttrs;
  }

  /**
   * This method evaluates sort criteria and returns an ArrayList of Object[] arrays of the
   * evaluated criteria.
   *
   * @param value the criteria to be evaluated.
   * @return an Object array of Object arrays of the evaluated criteria.
   */
  protected Object[] evaluateSortCriteria(Object value) {
    Object[] array = null;
    if (orderByAttrs != null) {
      array = new Object[orderByAttrs.size()];
      int i = 0;
      for (CompiledSortCriterion csc : orderByAttrs) {
        Object[] arr = {csc.evaluate(value, context), csc.getCriterion()};
        array[i++] = arr;
      }
    }
    return array;
  }

  /**
   * This method evaluates sort criteria and returns the resulting integer value of comparing the
   * two objects passed into it based on these criteria.
   *
   * @param value1 the first object to be compared.
   * @param value2 the second object to be compared.
   * @return a negative integer, zero, or a positive integer if the first argument is less than,
   *         equal to, or greater than the second, based on the evaluated sort criteria.
   */
  protected int evaluateSortCriteria(Object value1, Object value2) {
    int result = -1;
    if (orderByAttrs != null) {
      for (CompiledSortCriterion csc : orderByAttrs) {
        Object sortCriteriaForValue1 = csc.evaluate(value1, context);
        Object sortCriteriaForValue2 = csc.evaluate(value2, context);
        result = compareHelperMethod(sortCriteriaForValue1, sortCriteriaForValue2);
        if (result != 0) {
          if (csc.getCriterion()) {
            result *= -1;
          }
          break;
        }
      }
    }
    return result;
  }

  /**
   * Compares its two arguments for order. Returns a negative integer, zero, or a positive integer
   * as the first argument is less than, equal to, or greater than the second.
   *
   * @param obj1 the first object to be compared.
   * @param obj2 the second object to be compared.
   * @return a negative integer, zero, or a positive integer as the first argument is less than,
   *         equal to, or greater than the second.
   * @throws ClassCastException if the arguments' types prevent them from being compared by this
   *         Comparator.
   */
  @Override
  public int compare(Object obj1, Object obj2) {
    int result;
    if (obj1 == null && obj2 == null) {
      return 0;
    }
    assert !(obj1 instanceof VMCachedDeserializable || obj2 instanceof VMCachedDeserializable);
    if ((this.objType.isStructType() && obj1 instanceof Object[] && obj2 instanceof Object[])
        || !this.objType.isStructType()) {
      if (((result = evaluateSortCriteria(obj1, obj2)) != 0) && (orderByAttrs != null)) {
        return result;
      }
      context.getObserver().orderByColumnsEqual();
      // Comparable fields are equal - check if overall keys are equal
      if (this.objType.isStructType()) {
        int i = 0;
        for (Object o1 : (Object[]) obj1) {
          Object o2 = ((Object[]) obj2)[i++];
          result = compareHelperMethod(o1, o2);
          if (result != 0) {
            return result;
          }
        }
        return 0;
      } else {
        return compareTwoStrings(obj1, obj2);
      }
    }
    throw new ClassCastException(); // throw exception if args can't be compared w/this comparator
  }

  void addEvaluatedSortCriteria(Object row, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // No op
  }

  private int compareHelperMethod(Object obj1, Object obj2) {
    if (obj1 == null || obj2 == null) {
      return compareIfOneOrMoreNull(obj1, obj2);
    } else if (obj1 == QueryService.UNDEFINED || obj2 == QueryService.UNDEFINED) {
      return compareIfOneOrMoreQueryServiceUndefined(obj1, obj2);
    } else {
      return compareTwoObjects(obj1, obj2);
    }
  }

  private int compareIfOneOrMoreNull(Object obj1, Object obj2) {
    if (obj1 == null) {
      return obj2 == null ? 0 : -1;
    } else {
      return 1;
    }
  }

  private int compareIfOneOrMoreQueryServiceUndefined(Object obj1, Object obj2) {
    if (obj1 == QueryService.UNDEFINED) {
      return obj2 == QueryService.UNDEFINED ? 0 : -1;
    } else {
      return 1;
    }
  }

  private int compareTwoObjects(Object obj1, Object obj2) {
    if (obj1 instanceof Number && obj2 instanceof Number) {
      return compareTwoNumbers(obj1, obj2);
    } else {
      return compareTwoStrings(obj1, obj2);
    }
  }

  private int compareTwoNumbers(Object obj1, Object obj2) {
    Number num1 = (Number) obj1;
    Number num2 = (Number) obj2;
    return Double.compare(num1.doubleValue(), num2.doubleValue());
  }

  private int compareTwoStrings(Object obj1, Object obj2) {
    if (obj1 instanceof PdxString && obj2 instanceof String) {
      obj2 = new PdxString((String) obj2);
    } else if (obj2 instanceof PdxString && obj1 instanceof String) {
      obj1 = new PdxString((String) obj1);
    }
    if (obj1 instanceof Comparable) {
      return ((Comparable) obj1).compareTo(obj2);
    } else {
      return obj1.equals(obj2) ? 0 : -1;
    }
  }
}
