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
/*
 * StructSetOrResultsSet.java Utlity Class : Can be used to compare the results (StructSet OR
 * ResultsSet) under the scenario without/with Index Usage. Created on June 13, 2005, 11:16 AM
 */
package org.apache.geode.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledGroupBySelect;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.CompiledSortCriterion;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.OrderByComparator;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.util.ArrayUtils;

/**
 * Used by these tests:
 *
 * <li/>EquiJoinIntegrationTest
 * <li/>IUMRCompositeIteratorJUnitTest
 * <li/>IUMRMultiIndexesMultiRegionJUnitTest
 * <li/>IUMRShuffleIteratorsJUnitTest
 * <li/>IUMRSingleRegionJUnitTest
 * <li/>IndexCreationJUnitTest
 * <li/>IndexHintJUnitTest
 * <li/>IndexMaintainceJUnitTest
 * <li/>IndexUseJUnitTest
 * <li/>IndexedMergeEquiJoinScenariosJUnitTest
 * <li/>MultiRegionIndexUsageJUnitTest
 * <li/>NonDistinctOrderByPartitionedJUnitTest
 * <li/>NonDistinctOrderByReplicatedJUnitTest
 * <li/>NonDistinctOrderByTestImplementation
 * <li/>OrderByPartitionedJUnitTest
 * <li/>OrderByReplicatedJUnitTest
 * <li/>OrderByTestImplementation
 * <li/>QueryIndexUsingXMLDUnitTest
 * <li/>QueryREUpdateInProgressJUnitTest
 * <li/>QueryUsingFunctionContextDUnitTest
 * <li/>QueryUsingPoolDUnitTest
 * <li/>TestNewFunctionSSorRSIntegrationTest
 *
 * Also used by:
 *
 * <li/>GroupByTestImpl
 * <li/>PdxGroupByTestImpl
 * <li/>PRQueryDUnitHelper
 */
public class StructSetOrResultsSet {

  public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len, String queries[]) {
    CompareQueryResultsWithoutAndWithIndexes(r, len, false, queries);
  }

  /** Creates a new instance of StructSetOrResultsSet */
  public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len, boolean checkOrder,
      String queries[]) {

    Collection coll1;
    Collection coll2;
    for (int j = 0; j < len; j++) {
      checkSelectResultTypes((SelectResults) r[j][0], (SelectResults) r[j][1], queries[j]);
      checkResultSizes((SelectResults) r[j][0], (SelectResults) r[j][1], queries[j]);

      if (checkOrder) {
        coll2 = (((SelectResults) r[j][1]).asList());
        coll1 = (((SelectResults) r[j][0]).asList());
      } else {
        coll2 = (((SelectResults) r[j][1]).asSet());
        coll1 = (((SelectResults) r[j][0]).asSet());
      }
      compareResults(coll1, coll2, queries[j], checkOrder);
    }
  }

  public void compareExternallySortedQueriesWithOrderBy(String[] queries, Object[][] baseResults)
      throws Exception {
    for (int i = 0; i < queries.length; i++) {
      Query q;
      try {
        String query = queries[i];
        int indexOfOrderBy = query.indexOf("order ");
        query = query.substring(0, indexOfOrderBy);
        q = CacheUtils.getQueryService().newQuery(query);
        CacheUtils.getLogger().info("Executing query: " + query);
        baseResults[i][1] = q.execute();
        int unorderedResultsSize = ((SelectResults) baseResults[i][1]).size();
        if (unorderedResultsSize == 0) {
          fail(
              "The test results size is 0. It may not be validating anything. Please rewrite the test.");
        }
        Wrapper wrapper = getOrderByComparatorAndLimitForQuery(queries[i], unorderedResultsSize);
        if (wrapper.validationLevel != ValidationLevel.NONE) {
          Object[] externallySorted = ((SelectResults) baseResults[i][1]).toArray();
          if (wrapper.validationLevel != ValidationLevel.MATCH_ONLY) {
            Arrays.sort(externallySorted, wrapper.obc);
          }
          if (wrapper.limit != -1) {
            if (externallySorted.length > wrapper.limit) {
              Object[] newExternallySorted = new Object[wrapper.limit];
              System.arraycopy(externallySorted, 0, newExternallySorted, 0, wrapper.limit);
              externallySorted = newExternallySorted;
            }
          }
          StructSetOrResultsSet ssOrrs1 = new StructSetOrResultsSet();
          ssOrrs1.compareQueryResultsWithExternallySortedResults((SelectResults) baseResults[i][0],
              externallySorted, queries[i], wrapper);


        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new AssertionError(
            "query with index=" + i + " has failed. failed query=" + queries[i], e);
      }
    }
  }

  private void compareQueryResultsWithExternallySortedResults(SelectResults sr,
      Object[] externallySorted, String query, Wrapper wrapper) {

    if (sr.size() == externallySorted.length) {
      CacheUtils.log("Both SelectResults are of Same Size i.e.  Size= " + sr.size());
    } else {
      fail("FAILED:SelectResults size is different in both the cases. Size1=" + sr.size()
          + " Size2 = " + externallySorted.length + "; failed query=" + query);
    }

    Collection coll1 = null;
    coll1 = sr.asList();
    int j = 0;
    if (wrapper.validationLevel != ValidationLevel.ORDER_BY_ONLY) {
      for (Object o : externallySorted) {
        boolean removed = coll1.remove(o);
        if (!removed) {
          LogWriter logger = CacheUtils.getLogger();
          logger.error("order by inconsistency at element index = " + j);
          logger.error(" query result =****");
          logger.error(" query result =" + coll1.toString());
          logger.error(" query result elementType=" + sr.getCollectionType().getElementType());
          logger.error(" externally sorted result =****");
          logger.error(ArrayUtils.toString(externallySorted));

          fail("failed query due to element mismatch=" + query);
        }
        ++j;
      }
      assertTrue(coll1.isEmpty());
    }

    if (wrapper.validationLevel != ValidationLevel.MATCH_ONLY) {
      coll1 = sr.asList();
      Iterator itert1 = coll1.iterator();
      int i = 0;
      for (Object o : externallySorted) {

        if (wrapper.obc.compare(o, itert1.next()) != 0) {
          LogWriter logger = CacheUtils.getLogger();
          logger.error("order by inconsistency at element index = " + i);
          logger.error(" query result =****");
          logger.error(" query result =" + coll1.toString());
          logger.error(" externally sorted result =****");
          logger.error(ArrayUtils.toString(externallySorted));
          fail("failed query due to order mismatch=" + query);
        }
        ++i;
      }
    }
  }

  public Wrapper getOrderByComparatorAndLimitForQuery(String orderByQuery, int unorderedResultSize)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException {
    DefaultQuery q = (DefaultQuery) CacheUtils.getQueryService().newQuery(orderByQuery);
    CompiledSelect cs = q.getSimpleSelect();
    List<CompiledSortCriterion> orderByAttribs = null;
    if (cs.getType() == CompiledValue.GROUP_BY_SELECT) {
      Field originalOrderByMethod =
          CompiledGroupBySelect.class.getDeclaredField("originalOrderByClause");
      originalOrderByMethod.setAccessible(true);
      orderByAttribs = (List<CompiledSortCriterion>) originalOrderByMethod.get(cs);
    } else {
      orderByAttribs = cs.getOrderByAttrs();
    }
    ObjectType resultType = cs.getElementTypeForOrderByQueries();
    ExecutionContext context = new ExecutionContext(null, CacheUtils.getCache());
    final OrderByComparator obc = new OrderByComparator(orderByAttribs, resultType, context);
    Comparator baseComparator = obc;
    if (resultType.isStructType()) {
      baseComparator = new Comparator<Struct>() {
        @Override
        public int compare(Struct o1, Struct o2) {
          return obc.compare(o1.getFieldValues(), o2.getFieldValues());
        }
      };
    }
    final Comparator secondLevelComparator = baseComparator;
    final Comparator finalComparator = new Comparator() {

      @Override
      public int compare(Object o1, Object o2) {
        final boolean[] orderByColsEqual = new boolean[] {false};
        QueryObserverHolder.setInstance(new QueryObserverAdapter() {
          @Override
          public void orderByColumnsEqual() {
            orderByColsEqual[0] = true;
          }
        });
        int result = secondLevelComparator.compare(o1, o2);
        if (result != 0 && orderByColsEqual[0]) {
          result = 0;
        }
        return result;
      }

    };

    Field hasUnmappedOrderByColsField =
        CompiledSelect.class.getDeclaredField("hasUnmappedOrderByCols");
    hasUnmappedOrderByColsField.setAccessible(true);
    boolean skip = ((Boolean) hasUnmappedOrderByColsField.get(cs)).booleanValue();
    ValidationLevel validationLevel = ValidationLevel.ALL;

    int limit;

    if (cs.getType() == CompiledValue.GROUP_BY_SELECT) {
      Field limitCVField = CompiledGroupBySelect.class.getDeclaredField("limit");
      limitCVField.setAccessible(true);
      CompiledValue limitCV = (CompiledValue) limitCVField.get(cs);
      Method evaluateLimitMethod = CompiledSelect.class.getDeclaredMethod("evaluateLimitValue",
          ExecutionContext.class, CompiledValue.class);
      evaluateLimitMethod.setAccessible(true);
      limit = ((Integer) evaluateLimitMethod.invoke(null, context, limitCV)).intValue();
    } else {
      limit = cs.getLimitValue(null);
    }

    if (limit != -1 && limit < unorderedResultSize) {
      // chances are that results will not match
      if (skip) {
        validationLevel = ValidationLevel.NONE;
      } else {
        validationLevel = ValidationLevel.ORDER_BY_ONLY;
      }
    } else {
      if (skip) {
        validationLevel = ValidationLevel.MATCH_ONLY;
      }
    }
    return new Wrapper(finalComparator, limit, validationLevel);
  }

  enum ValidationLevel {
    ALL, ORDER_BY_ONLY, MATCH_ONLY, NONE
  }

  private static class Wrapper {
    final Comparator obc;
    final int limit;
    final ValidationLevel validationLevel;

    Wrapper(Comparator obc, int limit, ValidationLevel validationLevel) {
      this.obc = obc;
      this.limit = limit;
      this.validationLevel = validationLevel;
    }
  }

  /** Creates a new instance of StructSetOrResultsSet */
  public void CompareCountStarQueryResultsWithoutAndWithIndexes(Object[][] r, int len,
      boolean checkOrder, String queries[]) {

    Integer count1, count2;
    Iterator<Integer> itert1, itert2;
    SelectResults result1, result2;
    boolean exactMatch = true;
    for (int j = 0; j < len; j++) {
      result1 = ((SelectResults) r[j][0]);
      result2 = ((SelectResults) r[j][1]);
      assertEquals(queries[j], 1, result1.size());
      assertEquals(queries[j], 1, result2.size());

      checkSelectResultTypes((SelectResults) r[j][0], (SelectResults) r[j][1], queries[j]);
      checkResultSizes((SelectResults) r[j][0], (SelectResults) r[j][1], queries[j]);
      compareResults(result1, result2, queries[j], true);
    }
  }

  /**
   * Compares two ArrayLists containing query results with/without order.
   */
  public void CompareQueryResultsAsListWithoutAndWithIndexes(Object[][] r, int len,
      boolean checkOrder, String queries[]) {
    CompareQueryResultsAsListWithoutAndWithIndexes(r, len, checkOrder, true, queries);
  }

  public void CompareQueryResultsAsListWithoutAndWithIndexes(Object[][] r, int len,
      boolean checkOrder, boolean checkClass, String queries[]) {
    Integer count1, count2;
    Iterator<Integer> itert1, itert2;
    ArrayList result1, result2;

    for (int j = 0; j < len; j++) {
      result1 = ((ArrayList) r[j][0]);
      result2 = ((ArrayList) r[j][1]);
      result1.trimToSize();
      result2.trimToSize();

      compareQueryResultLists(result1, result2, len, checkOrder, checkClass, queries[j]);
    }
  }

  public void compareQueryResultLists(List r1, List r2, int len, boolean checkOrder,
      boolean checkClass, String query) {
    if (checkClass) {
      if ((r1.get(0).getClass().getName()).equals(r2.get(0).getClass().getName())) {
        CacheUtils.log(
            "Both SelectResults are of the same Type i.e.--> " + r1.get(0).getClass().getName());
      } else {
        fail("FAILED:Select result Type is different in both the cases."
            + r1.get(0).getClass().getName() + "and" + r1.get(0).getClass().getName()
            + "; failed query=" + query);
      }
    }

    checkResultSizes(r1, r2, query);
    compareResults(r1, r2, query, checkOrder);
  }

  private void checkSelectResultTypes(SelectResults r1, SelectResults r2, String query) {
    ObjectType type1, type2;
    type1 = r1.getCollectionType().getElementType();
    type2 = r2.getCollectionType().getElementType();
    if (!(type1.getClass().getName()).equals(type2.getClass().getName())) {
      CacheUtils
          .log("Classes are : " + type1.getClass().getName() + " " + type2.getClass().getName());
      fail("FAILED:Select result Type is different in both the cases." + "; failed query=" + query);
    }
  }

  private void checkResultSizes(Collection r1, Collection r2, String query) {
    if (r1.size() != r2.size()) {
      fail("FAILED:SelectResults size is different in both the cases. Size1=" + r1.size()
          + " Size2 = " + r2.size() + "; failed query=" + query);
    }
  }

  private void compareResults(Collection result1, Collection result2, String query,
      boolean checkOrder) {
    Iterator itert1 = result1.iterator();
    Iterator itert2 = result2.iterator();
    int currentIndex = 0;
    while (itert1.hasNext()) {
      Object p1 = itert1.next();
      Object p2 = null;
      if (!checkOrder) {
        if (!collectionContains(result2, p1)) {
          fail("Atleast one element in the pair of SelectResults "
              + "supposedly identical, is not equal " + "Match not found for :" + p1
              + " compared with:" + p2 + "; failed query=" + query + "; element unmatched =" + p1
              + ";p1 class=" + p1.getClass() + " ; other set has =" + result2);
        }
      } else {
        boolean matched = false;
        if (itert2.hasNext()) {
          p2 = itert2.next();
          matched = objectsEqual(p1, p2);
          if (!matched) {
            fail("Order of results was not the same, match not found for :" + p1 + " compared with:"
                + p2 + "; failed query=" + query + "; element unmatched =" + p1 + ";p1 class="
                + p1.getClass() + " compared with " + p2 + ";p2 class=" + p2.getClass()
                + "currentIndex:" + currentIndex + "\nr1:" + result1 + "\n\nr2:" + result2);
          }
        }
      }
      currentIndex++;
    }
  }

  private boolean collectionContains(Collection collection, Object object) {
    Iterator iterator = collection.iterator();
    while (iterator.hasNext()) {
      Object o = iterator.next();
      if (objectsEqual(object, o)) {
        return true;
      }
    }
    return false;
  }

  private boolean objectsEqual(Object o1, Object o2) {
    // Assumed that o1 and o2 are the same object type as both are from collections created by
    // executing the same query
    if (o1 instanceof Struct) {
      // if o2 is null, an NPE will be thrown.
      Object[] values1 = ((Struct) o1).getFieldValues();
      Object[] values2 = ((Struct) o2).getFieldValues();
      assertEquals(values1.length, values2.length);
      boolean elementEqual = true;
      for (int i = 0; i < values1.length; ++i) {
        elementEqual =
            elementEqual && ((values1[i] == values2[i]) || values1[i].equals(values2[i]));
      }
      if (elementEqual) {
        return true;
      }
    } else {
      // if o1 is null and o2 is not, an NPE will be thrown
      if (o1 == o2 || o1.equals(o2)) {
        return true;
      }
    }
    return false;
  }
}
