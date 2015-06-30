/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * StructSetOrResultsSet.java
 * Utlity Class : Can be used to compare the results (StructSet OR ResultsSet) under the scenario without/with Index Usage.
 * Created on June 13, 2005, 11:16 AM
 */

package com.gemstone.gemfire.cache.query.functional;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.CompiledGroupBySelect;
import com.gemstone.gemfire.cache.query.internal.CompiledSelect;
import com.gemstone.gemfire.cache.query.internal.CompiledSortCriterion;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.OrderByComparator;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.util.ArrayUtils;

/**
 * @author vikramj, shobhit
 */
public class StructSetOrResultsSet extends TestCase {

 
  public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len,
      String queries[]) {
    CompareQueryResultsWithoutAndWithIndexes(r, len, false, queries);
  }

  /** Creates a new instance of StructSetOrResultsSet */
  public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len,
      boolean checkOrder, String queries[]) {

    Collection coll1 = null;
    Collection coll2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    for (int j = 0; j < len; j++) {
      type1 = ((SelectResults) r[j][0]).getCollectionType().getElementType();
      type2 = ((SelectResults) r[j][1]).getCollectionType().getElementType();
      if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
        CacheUtils.log("Both SelectResults are of the same Type i.e.--> "
            + ((SelectResults) r[j][0]).getCollectionType().getElementType());
      } else {
        CacheUtils.log("Classes are : " + type1.getClass().getName() + " "
            + type2.getClass().getName());
        fail("FAILED:Select result Type is different in both the cases."
            + "; failed query=" + queries[j]);
      }
      if (((SelectResults) r[j][0]).size() == ((SelectResults) r[j][1]).size()) {
        CacheUtils.log("Both SelectResults are of Same Size i.e.  Size= "
            + ((SelectResults) r[j][1]).size());
      } else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + ((SelectResults) r[j][0]).size() + " Size2 = "
            + ((SelectResults) r[j][1]).size() + "; failed query=" + queries[j]);
      }
      
      if (checkOrder) {
        coll2 = (((SelectResults) r[j][1]).asList());
        coll1 = (((SelectResults) r[j][0]).asList());
      } else {
        coll2 = (((SelectResults) r[j][1]).asSet());
        coll1 = (((SelectResults) r[j][0]).asSet());
      }
      // boolean pass = true;
      itert1 = coll1.iterator();
      itert2 = coll2.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        if (!checkOrder) {
          itert2 = coll2.iterator();
        }

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              // CacheUtils.log("Comparing: " + values1[i] + " with: " +
              // values2[i]);
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            // CacheUtils.log("Comparing: " + p1 + " with: " + p2);
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch || checkOrder) {
            break;
          }
        }
        if (!exactMatch) {
          fail("Atleast one element in the pair of SelectResults "
              + "supposedly identical, is not equal " + "Match not found for :"
              + p1 + "; failed query=" + queries[j] + "; element unmatched ="
              + p1 + ";p1 class=" + p1.getClass() + " ; other set has ="
              + coll2);
        }
      }
    }
  }

  public void compareExternallySortedQueriesWithOrderBy(String[] queries,
      Object[][] baseResults) throws Exception {
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        String query = queries[i];
        int indexOfOrderBy = query.indexOf("order ");
        query = query.substring(0, indexOfOrderBy);
        q = CacheUtils.getQueryService().newQuery(query);
        CacheUtils.getLogger().info("Executing query: " + query);
        baseResults[i][1] = q.execute();
        int unorderedResultsSize =  ((SelectResults) baseResults[i][1]).size(); 
        if(unorderedResultsSize == 0) {
          fail("The test results size is 0 , it possibly is not validating anything. rewrite the test");
        }
        Wrapper wrapper = getOrderByComparatorAndLimitForQuery(queries[i],unorderedResultsSize );
        if (wrapper.validationLevel != ValidationLevel.NONE) {
          Object[] externallySorted = ((SelectResults) baseResults[i][1])
              .toArray();
          if (wrapper.validationLevel != ValidationLevel.MATCH_ONLY) {
            Arrays.sort(externallySorted, wrapper.obc);
          }
          if (wrapper.limit != -1) {
            if (externallySorted.length > wrapper.limit) {
              Object[] newExternallySorted = new Object[wrapper.limit];
              System.arraycopy(externallySorted, 0, newExternallySorted, 0,
                  wrapper.limit);
              externallySorted = newExternallySorted;
            }
          }
          StructSetOrResultsSet ssOrrs1 = new StructSetOrResultsSet();
          ssOrrs1.compareQueryResultsWithExternallySortedResults(
              (SelectResults) baseResults[i][0], externallySorted, queries[i],
              wrapper);


        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("query with index=" + i + " has failed. failed query="
            + queries[i]);
      }

    }
  }

  private void compareQueryResultsWithExternallySortedResults(SelectResults sr,
      Object[] externallySorted, String query, Wrapper wrapper) {

    if (sr.size() == externallySorted.length) {
      CacheUtils.log("Both SelectResults are of Same Size i.e.  Size= "
          + sr.size());
    } else {
      fail("FAILED:SelectResults size is different in both the cases. Size1="
          + sr.size() + " Size2 = " + externallySorted.length
          + "; failed query=" + query);
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

  public Wrapper getOrderByComparatorAndLimitForQuery(String orderByQuery,
      int unorderedResultSize) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    DefaultQuery q = (DefaultQuery) CacheUtils.getQueryService().newQuery(
        orderByQuery);
    CompiledSelect cs = q.getSimpleSelect();    
    List<CompiledSortCriterion> orderByAttribs = null;
    if(cs.getType() == CompiledValue.GROUP_BY_SELECT) {
      Field originalOrderByMethod = CompiledGroupBySelect.class.getDeclaredField("originalOrderByClause");
      originalOrderByMethod.setAccessible(true);
      orderByAttribs = ( List<CompiledSortCriterion>)originalOrderByMethod.get(cs);
    }else {
      orderByAttribs = cs.getOrderByAttrs();
    }
    ObjectType resultType = cs.getElementTypeForOrderByQueries();
    ExecutionContext context = new ExecutionContext(null, CacheUtils.getCache());
    final OrderByComparator obc = new OrderByComparator(orderByAttribs,
        resultType, context);
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
        final boolean[] orderByColsEqual = new boolean[] { false };
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

    Field hasUnmappedOrderByColsField = CompiledSelect.class
        .getDeclaredField("hasUnmappedOrderByCols");
    hasUnmappedOrderByColsField.setAccessible(true);
    boolean skip = ((Boolean) hasUnmappedOrderByColsField.get(cs))
        .booleanValue();
    ValidationLevel validationLevel = ValidationLevel.ALL;
    
    int limit ;
    
    if(cs.getType() == CompiledValue.GROUP_BY_SELECT) {
      Field limitCVField = CompiledGroupBySelect.class.getDeclaredField("limit");
      limitCVField.setAccessible(true);
      CompiledValue limitCV = ( CompiledValue)limitCVField.get(cs);
      Method evaluateLimitMethod = CompiledSelect.class.getDeclaredMethod("evaluateLimitValue",
          ExecutionContext.class, CompiledValue.class );
      evaluateLimitMethod.setAccessible(true);
      limit = ((Integer)evaluateLimitMethod.invoke(null, context, limitCV)).intValue();
    }else {
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
  public void CompareCountStarQueryResultsWithoutAndWithIndexes(Object[][] r,
      int len, boolean checkOrder, String queries[]) {

    Integer count1, count2;
    Iterator<Integer> itert1, itert2;
    SelectResults result1, result2;
    boolean exactMatch = true;
    for (int j = 0; j < len; j++) {
      result1 = ((SelectResults) r[j][0]);
      result2 = ((SelectResults) r[j][1]);
      assertEquals(queries[j], 1, result1.size());
      assertEquals(queries[j], 1, result2.size());

      if ((result1.asList().get(0).getClass().getName()).equals(result2
          .asList().get(0).getClass().getName())) {
        CacheUtils.log("Both SelectResults are of the same Type i.e.--> "
            + ((SelectResults) r[j][0]).getCollectionType().getElementType());
      } else {
        fail("FAILED:Select result Type is different in both the cases."
            + "; failed query=" + queries[j]);
      }

      if (((SelectResults) r[j][0]).size() == ((SelectResults) r[j][1]).size()) {
        CacheUtils.log("Both SelectResults are of Same Size i.e.  Size= "
            + ((SelectResults) r[j][1]).size());
      } else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + ((SelectResults) r[j][0]).size() + " Size2 = "
            + ((SelectResults) r[j][1]).size() + "; failed query=" + queries[j]);
      }

      // boolean pass = true;
      itert1 = result1.iterator();
      itert2 = result2.iterator();
      while (itert1.hasNext()) {
        Integer p1 = itert1.next();
        Integer p2 = itert2.next();
        CacheUtils.log("result1: " + p1 + "result2: " + p2);
        exactMatch &= p1.intValue() == p2.intValue();

      }
      if (!exactMatch) {
        fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal "
            + "; failed query=" + queries[j]);
      }
    }
  }

  /**
   * Compares two ArrayLists containing query results with/without order.
   *
   * @param r
   *          Array of ArrayLists
   * @param len
   *          Length of array of ArrayLists
   * @param checkOrder
   * @param queries
   */
  public void CompareQueryResultsAsListWithoutAndWithIndexes(Object[][] r,
      int len, boolean checkOrder, String queries[]) {
    CompareQueryResultsAsListWithoutAndWithIndexes(r, len, checkOrder, true,
        queries);
  }

  public void CompareQueryResultsAsListWithoutAndWithIndexes(Object[][] r,
      int len, boolean checkOrder, boolean checkClass, String queries[]) {
    Integer count1, count2;
    Iterator<Integer> itert1, itert2;
    ArrayList result1, result2;
    for (int j = 0; j < len; j++) {
      result1 = ((ArrayList) r[j][0]);
      result2 = ((ArrayList) r[j][1]);
      result1.trimToSize();
      result2.trimToSize();
      // assertFalse(queries[j], result1.size()==0);
      // assertFalse(queries[j], result2.size()==0);

      if (checkClass) {
        if ((result1.get(0).getClass().getName()).equals(result2.get(0)
            .getClass().getName())) {
          CacheUtils.log("Both SelectResults are of the same Type i.e.--> "
              + result1.get(0).getClass().getName());
        } else {
          fail("FAILED:Select result Type is different in both the cases."
              + result1.get(0).getClass().getName() + "and"
              + result1.get(0).getClass().getName() + "; failed query="
              + queries[j]);
        }
      }

      if (result1.size() == result2.size()) {
        CacheUtils.log("Both SelectResults are of Same Size i.e.  Size= "
            + result2.size());
      } else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + result1.size() + " Size2 = " + result2.size() + "; failed query="
            + queries[j]);
      }

      // boolean pass = true;
      itert1 = result1.iterator();
      itert2 = result2.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        if (!checkOrder) {
          itert2 = result2.iterator();
        }

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch || checkOrder) {
            break;
          }
        }
        if (!exactMatch) {
          fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal "
              + "; failed query=" + queries[j]);
        }
      }
    }
  }
}
