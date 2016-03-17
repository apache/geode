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
/*
 * QueryWithBucketParameterTest.java
 * JUnit based test
 *
 * Created on April 13, 2005, 2:40 PM
 */
package com.gemstone.gemfire.cache.query;

        import static org.junit.Assert.*;

        import java.io.Serializable;
        import java.util.*;

        import org.junit.After;
        import org.junit.Before;
        import org.junit.Rule;
        import org.junit.Test;
        import org.junit.experimental.categories.Category;

        import com.gemstone.gemfire.cache.*;
        import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
        import com.gemstone.gemfire.internal.cache.LocalDataSet;
        import com.gemstone.gemfire.internal.cache.PartitionedRegion;
        import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
        import org.junit.rules.ExpectedException;

/**
 * @implNote This test class has been designed to test the query execution using the LocalDataSet.executeQuery
 * where one of the parameters passed is the bucket set to be used.
 * Different variation of hashset variables are passed to the function to check for errors.
 * Created by nnag on 3/3/16.
 */
@Category(IntegrationTest.class)
public class QueryWithBucketParameterIntgTest {

    //Necessary data elements required for execution of the test
//    Region region;
//    QueryService qs;
//    Cache cache;
    DefaultQuery queryExecutor;
    LocalDataSet lds;

    /**
     * @implNote This code is copied from BugJUnitTest.java. These elements are required to pass as parameters
     * to create data elements for the region
     * @implNote This needs to be moved to a common test package to avoid redundant code.
     */
    static class MyValue implements Serializable, Comparable<MyValue>
    {
        public int value = 0;

        public MyValue(int value) {
            this.value = value;
        }


        public int compareTo(MyValue o)
        {
            if(this.value > o.value) {
                return 1;
            }else if(this.value < o.value) {
                return -1;
            }else {
                return 0;
            }
        }
    }

    //Empty constructor
    public QueryWithBucketParameterIntgTest() {
    }

    /**
     * Pre-test execution to create the test bed.
     * This code snippet has been borrowed from (com.gemstone.gemfire.cache.query) BugJUintTest.java [03/02/2016]
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        String regionName = "pr1";
        int totalBuckets = 40;
        int numValues = 80;
        CacheUtils.startCache();
        Cache cache = CacheUtils.getCache();
        PartitionAttributesFactory pAFactory = getPartitionAttributesFactoryWithPartitionResolver(totalBuckets);
        RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION);
        rf.setPartitionAttributes(pAFactory.create());
        PartitionedRegion pr1 = (PartitionedRegion) rf.create(regionName);
        populateRegion(pr1, numValues);
        QueryService qs = pr1.getCache().getQueryService();
        String query = "select distinct e1.value from /pr1 e1";
        queryExecutor = (DefaultQuery)CacheUtils.getQueryService().newQuery(
                query);
        Set<Integer> set = getBucketList(totalBuckets);
        lds = new LocalDataSet(pr1, set);
    }

    private Set<Integer> getBucketList(int nBuckets) {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < nBuckets; ++i) {
            set.add(i);
        }
        return set;
    }

    private PartitionAttributesFactory getPartitionAttributesFactoryWithPartitionResolver(int totalBuckets) {
        PartitionAttributesFactory pAFactory = new PartitionAttributesFactory();
        pAFactory.setRedundantCopies(1).setTotalNumBuckets(totalBuckets).setPartitionResolver(
                getPartitionResolver());
        return pAFactory;
    }

    private PartitionResolver getPartitionResolver() {
        return new PartitionResolver() {

            public String getName()
            {
                return "PartitionResolverForTest";
            }

            public Serializable getRoutingObject(EntryOperation opDetails)
            {
                return (Serializable)opDetails.getKey();
            }

            public void close()
            {

            }

        };
    }

    private void populateRegion(Region region, int numValues) {
        for (int i = 1; i <= numValues; i++) {
            region.put(i, new MyValue(i));

        }
    }

    /**
     * Post test completion clean-up/tear down of the test bed
     * This code snippet has been borrowed from (com.gemstone.gemfire.cache.query) BugJUintTest.java [03/02/2016]
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        CacheUtils.closeCache();
    }

    /**
     * remark: Query is executed with an empty bucket parameter
     * expected behaviour : A query executed on empty hashset as its bucket set must return no result.
     * @throws Exception
     */
    @Test
    public void testQueryExecuteWithEmptyBucketListExpectNoResults() throws Exception
    {

        SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, new HashSet<Integer>());
        assertTrue("Received: A non-empty result collection, expected : Empty result collection", r.isEmpty());

    }
    /**
     * remark: Query is executed with an null bucket parameter
     * expected behaviour : the query is executed on all of its available bucket and set must return no result.
     * null bucket parameter will set the engine to operate in default mode, which means all of regions available
     * buckets. Hence result should not be empty.
     * @throws Exception
     */
    @Test
    public void testQueryExecuteWithNullBucketListExpectNonEmptyResultSet() throws Exception
    {

        SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, null);
        assertFalse("Received: An empty result collection, expected : Non-empty result collection", r.isEmpty());

    }
    /**
     * remark: Query is executed with an valid non-empty bucket parameter
     * expected behaviour : the query is executed on valid buckets hence returns valid non empty result.
     * @throws Exception
     */
    @Test
    public void testQueryExecuteWithNonEmptyBucketListExpectNonEmptyResultSet() throws Exception
    {
        int nTestBucketNumber = 15;
        Set<Integer> nonEmptySet = getBucketList(nTestBucketNumber);
        SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, nonEmptySet);
        assertFalse("Received: An empty result collection, expected : Non-empty result collection", r.isEmpty());


    }

    /**
     * Expected an exception when a larger bucket list is passed as a parameter than the actual bucket size in the
     * region
     * @throws Exception
     */

    @Test(expected = QueryInvocationTargetException.class)
    public void testQueryExecuteWithLargerBucketListThanExistingExpectQueryInvocationTargetException() throws Exception
    {
        int nTestBucketNumber = 45;
        Set<Integer> overflowSet = getBucketList(nTestBucketNumber);
        SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, overflowSet); 
    }
}