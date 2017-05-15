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
import java.util.Iterator;
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
 * A generic comparator class which compares two Object/StructImpl according to their sort criterion
 * specified in order by clause
 * 
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
   * Yogesh : This methods evaluates sort criteria and returns a ArrayList of Object[] arrays of
   * evaluated criteria
   * 
   * @param value
   * @return Object[]
   */
  protected Object[] evaluateSortCriteria(Object value) {

    CompiledSortCriterion csc;
    Object[] array = null;
    if (orderByAttrs != null) {
      array = new Object[orderByAttrs.size()];
      Iterator orderiter = orderByAttrs.iterator();
      int i = 0;
      while (orderiter.hasNext()) {
        csc = (CompiledSortCriterion) orderiter.next();
        Object[] arr = new Object[2];
        arr[0] = csc.evaluate(value, context);
        arr[1] = Boolean.valueOf(csc.getCriterion());
        array[i++] = arr;
      }

    }
    return array;
  }

  protected int evaluateSortCriteria(Object value1, Object value2) {
    int result = -1;
    CompiledSortCriterion csc;
    if (orderByAttrs != null) {
      Iterator orderiter = orderByAttrs.iterator();
      while (orderiter.hasNext()) {
        csc = (CompiledSortCriterion) orderiter.next();
        Object sortCriteriaForValue1 = csc.evaluate(value1, context);
        Object sortCriteriaForValue2 = csc.evaluate(value2, context);

        if (sortCriteriaForValue1 == null || sortCriteriaForValue2 == null) {
          if (sortCriteriaForValue1 == null) {
            result = (sortCriteriaForValue2 == null ? 0 : -1);
          } else {
            result = 1;
          }
        } else if (sortCriteriaForValue1 == QueryService.UNDEFINED
            || sortCriteriaForValue2 == QueryService.UNDEFINED) {
          if (sortCriteriaForValue1 == QueryService.UNDEFINED) {
            result = (sortCriteriaForValue2 == QueryService.UNDEFINED ? 0 : -1);
          } else {
            result = 1;
          }
        } else {
          if (sortCriteriaForValue1 instanceof Number && sortCriteriaForValue2 instanceof Number) {
            double diff = ((Number) sortCriteriaForValue1).doubleValue()
                - ((Number) sortCriteriaForValue2).doubleValue();
            result = diff > 0 ? 1 : diff < 0 ? -1 : 0;
          } else {
            if (sortCriteriaForValue1 instanceof PdxString
                && sortCriteriaForValue2 instanceof String) {
              sortCriteriaForValue2 = new PdxString((String) sortCriteriaForValue2);
            } else if (sortCriteriaForValue2 instanceof PdxString
                && sortCriteriaForValue1 instanceof String) {
              sortCriteriaForValue1 = new PdxString((String) sortCriteriaForValue1);
            }
            result = ((Comparable) sortCriteriaForValue1).compareTo(sortCriteriaForValue2);
          }

        }

        if (result == 0) {
          continue;
        } else {
          if (Boolean.valueOf(csc.getCriterion())) {
            result = (result * (-1));
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
  public int compare(Object obj1, Object obj2) {
    int result = -1;
    if (obj1 == null && obj2 == null) {
      return 0;
    }
    assert !(obj1 instanceof VMCachedDeserializable || obj2 instanceof VMCachedDeserializable);

    if ((this.objType.isStructType() && obj1 instanceof Object[] && obj2 instanceof Object[])
        || !this.objType.isStructType()) { // obj1 instanceof Object && obj2
                                           // instanceof Object){
      if ((result = evaluateSortCriteria(obj1, obj2)) != 0) {
        return result;
      }

      QueryObserver observer = QueryObserverHolder.getInstance();
      if (observer != null) {
        observer.orderByColumnsEqual();
      }
      // The comparable fields are equal, so we check if the overall keys are
      // equal or not
      if (this.objType.isStructType()) {
        int i = 0;
        for (Object o1 : (Object[]) obj1) {
          Object o2 = ((Object[]) obj2)[i++];

          // Check for null value.
          if (o1 == null || o2 == null) {
            if (o1 == null) {
              if (o2 == null) {
                continue;
              }
              return -1;
            } else {
              return 1;
            }
          } else if (o1 == QueryService.UNDEFINED || o2 == QueryService.UNDEFINED) {
            if (o1 == QueryService.UNDEFINED) {
              if (o2 == QueryService.UNDEFINED) {
                continue;
              }
              return -1;
            } else {
              return 1;
            }
          }

          if (o1 instanceof Comparable) {
            final int rslt;
            if (o1 instanceof Number && o2 instanceof Number) {
              double diff = ((Number) o1).doubleValue() - ((Number) o2).doubleValue();
              rslt = diff > 0 ? 1 : diff < 0 ? -1 : 0;
            } else {
              if (o1 instanceof PdxString && o2 instanceof String) {
                o2 = new PdxString((String) o2);
              } else if (o2 instanceof PdxString && o1 instanceof String) {
                o1 = new PdxString((String) o1);
              }
              rslt = ((Comparable) o1).compareTo(o2);
            }
            if (rslt == 0) {
              continue;
            } else {
              return rslt;
            }
          } else if (!o1.equals(o2)) {
            return -1;
          }
        }
        return 0;
      } else {
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
    return -1;
  }

  void addEvaluatedSortCriteria(Object row, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // No op
  }

}
