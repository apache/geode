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
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test verifies Region#query()for PartitionedRegion on a single VM with
 * Region#close() being called on the same with some delay and then recreating the region instantly.
 * 
 * @author pbatra
 * 
 */
@Category(IntegrationTest.class)
public class PRQueryRegionClosedJUnitTest
{
  static final String regionName = "portfolios";

  static final String localRegionName = "localPortfolios";

  static final int redundancy = 0;

  static final String localMaxMemory = "100";

  LogWriter logger = null;

  boolean encounteredException = false;
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
   * 4. Create another Thread and call Region#close() on the PR region.<br>
   * 
   * 
   * @throws Exception
   */
  @Test
  public void testQueryingWithRegionClose() throws Exception
  {

    logger
        .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Test Started  ");

    logger
        .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: creating PR Region ");

    final Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, localMaxMemory, redundancy);

    final Region localRegion = PartitionedRegionTestHelper
        .createLocalRegion(localRegionName);

    final StringBuffer errorBuf = new StringBuffer("");

    PortfolioData[] portfolios = new PortfolioData[100];

    try {
      for (int j = 0; j < 100; j++) {
        portfolios[j] = new PortfolioData(j);
      }
      logger
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: populating PortfolioData into the PR Datastore  ");

      populateData(region, portfolios);

      logger
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: populating PortfolioData into the PR Datastore  ");

      populateData(localRegion, portfolios);
      final String[] queryString = { "ID = 0 OR ID = 1", "ID > 4 AND ID < 9",
          "ID = 5", "ID < 5 ", "ID <= 5" };

      logger
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Creating a Thread which will fire queries on the datastore");

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

            catch (RegionDestroyedException rde) {
              logger
                  .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: RegionDestroyedException as Expected "
                      + rde);

            }
            catch (RegionNotFoundException rnfe) {
              logger
              .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: RegionNotFoundException as Expected "
                    + rnfe);
              
            }
            
            catch (QueryInvocationTargetException qite) {
              logger
              .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: QueryInvocationTargetException as Expected "
                    + qite);
              
            }
            catch (Exception qe) {
              logger
                  .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Unexpected Exception "
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
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Creating a Thread which will call Region.destroyRegion() on the datastore ");

      Thread t2 = new Thread(new Runnable() {
        public void run()
        {
          try {
            Thread.sleep(2500);
          }
          catch (InterruptedException ie) {
            fail("interrupted");
          }
         region.close();
          
          logger
          .info(
              "PROperationWithQueryDUnitTest#getCacheSerializableRunnableForRegionClose: Region Closed on VM ");
          
          
        }

      });

      logger
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Initiating the  Threads");

      t1.start();
      t2.start();

      logger
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Waiting for the Threads to join ");

      t1.join(30000);
      assertFalse(t1.isAlive());
      t2.join(30000);
      assertFalse(t2.isAlive());
      logger
          .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: checking for any Unexpected Exception's occured");

      assertFalse(
          "PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Exception occured in Query-thread",
          encounteredException);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Test failed because of exception "
          + e);

    }

    logger
        .info("PRQueryRegionClosedJUnitTest#testQueryingWithRegionClose: Test Ended");

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
        .info("PRQueryRegionClosedJUnitTest#populateData: Populating Data in the PR Region ");
    for (int j = 0; j < data.length; j++) {
      region.put(new Integer(j), data[j]);
    }
  }
}
