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
package com.gemstone.gemfire.cache.query.partitioned;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test verifies Region#query()for PartitionedRegion on a single VM with
 * Region#destroyRegion() being called on the same with some delay.
 * 
 * @author pbatra
 * 
 */
@Category(IntegrationTest.class)
public class PRQueryRegionDestroyedJUnitTest
{
  // PR Region name
  static final String regionName = "portfolios";

  // Local Region name 
  static final String localRegionName = "localPortfolios";

  // redundancy level for PR
  static final int redundancy = 0;

  // local max memory for PR  
  static final String localMaxMemory = "100";

  LogWriter logger = null;
  boolean encounteredException = false;
  static final int dataSize =100; 
  static final int delayQuery = 1000;

  @Before
  public void setUp() throws Exception
  {
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }
  }

  /**
   * Tests the execution of query on a PartitionedRegion created on a single
   * data store. <br>
   * 1. Creates a PR with redundancy=0 on a single VM. <br>
   * 2. Puts some test Objects in cache.<br>
   * 3. Create a Thread and fire queries on the data and verifies the result.<br>
   * 4. Create another Thread and call Region#destroyRegion() on the PR region.<br>
   * 
   * 
   * @throws Exception
   */
  @Test
  public void testQueryOnSingleDataStore() throws Exception
  {

    logger
        .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Test Started  ");

    logger
        .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: creating PR Region ");

    final Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, localMaxMemory, redundancy);

    final Region localRegion = PartitionedRegionTestHelper
        .createLocalRegion(localRegionName);

    final StringBuffer errorBuf = new StringBuffer("");

    PortfolioData[] portfolios = new PortfolioData[dataSize];

    try {
      for (int j = 0; j < dataSize; j++) {
        portfolios[j] = new PortfolioData(j);
      }
      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: populating PortfolioData into the PR Datastore  ");

      populateData(region, portfolios);

      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: populating PortfolioData into the PR Datastore  ");

      populateData(localRegion, portfolios);
      final String[] queryString = { "ID = 0 OR ID = 1", "ID > 4 AND ID < 9",
          "ID = 5", "ID < 5 ", "ID <= 5" };

      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Creating a Thread which will fire queries on the datastore");

      Thread t1 = new Thread(new Runnable() {
        public void run()
        {
          final String expectedRegionDestroyedException = RegionDestroyedException.class
              .getName();

          logger.info("<ExpectedException action=add>"
              + expectedRegionDestroyedException + "</ExpectedException>");

          for (int i = 0; i < queryString.length; i++) {

            try {

              SelectResults resSetPR = region.query(queryString[i]);
              SelectResults resSetLocal = localRegion.query(queryString[i]);
              String failureString=PartitionedRegionTestHelper.compareResultSets(resSetPR,
                  resSetLocal);
              Thread.sleep(delayQuery);
              if(failureString!=null){
                errorBuf.append(failureString);
                throw (new Exception(failureString));
                
              }

            }
            catch (InterruptedException ie) {
              fail("interrupted");
            }

            catch (QueryInvocationTargetException qite) {
              logger
                  .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: QueryInvocationTargetException as Expected "
                      + qite);

            }
            catch (RegionDestroyedException rde) {
              logger
                  .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: RegionDestroyedException as Expected "
                      + rde);

            }
            catch (RegionNotFoundException rnfe) {
              logger
              .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: RegionNotFoundException as Expected "
                    + rnfe);
              
            }
            
            catch (Exception qe) {
              logger
                  .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Unexpected Exception "
                      + qe);

              encounteredException = true;
              StringWriter sw = new StringWriter();
              qe.printStackTrace(new PrintWriter(sw));
              errorBuf.append(sw);

            }

          }
          logger.info("<ExpectedException action=remove>"
              + expectedRegionDestroyedException + "</ExpectedException>");

        }
      });
      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Creating a Thread which will call Region.destroyRegion() on the datastore ");

      Thread t2 = new Thread(new Runnable() {
        public void run()
        {
          try {
            Thread.sleep(2500);
          }
          catch (InterruptedException ie) {
            logger
                .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore:Thread Interrupted Exceptionduring region Destroy ");
            fail("interrupted");
          }
          region.destroyRegion();
        }

      });

      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Initiating the  Threads");

      t1.start();
      t2.start();

      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Waiting for the Threads to join ");

      ThreadUtils.join(t1, 30 * 1000);
      ThreadUtils.join(t2, 30 * 1000);
      logger
          .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: checking for any Unexpected Exception's occured");

      assertFalse(
          "PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Exception occured in Query-thread",
          encounteredException);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Test failed because of exception "
          + e);

    }

    logger
        .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStore: Test Ended");

  }

  /**
   * Populates the region with the Objects stores in the data Object array.
   * 
   * @param region
   * @param data
   */
  private void populateData(Region region, Object[] data)
  {
    logger
        .info("PRQueryRegionDestroyedJUnitTest#populateData: Populating Data in the PR Region ");
    for (int j = 0; j < data.length; j++) {
      region.put(new Integer(j), data[j]);
    }
  }
}
