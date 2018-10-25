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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.security.query.QuerySecurityBase;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CqSecurityUnauthorizedUserDUnitTest extends QuerySecurityBase {

  @Parameterized.Parameters
  public static Object[] usersAllowed() {
    return new Object[] {"stranger", "dataReaderRegionKey", "clusterManagerQuery",
        "clusterManagerDataReaderRegionKey", "dataWriter"};
  }

  @Parameterized.Parameter
  public String user;

  @Before
  public void configureSpecificClientAndKeyAndValues() {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0"};
    values = new Object[] {new QueryTestObject(0, "John")};
  }

  // Variables that need to be shared across invoke calls.
  protected static CqSecurityTestCqListener cqListener = null;

  public List<String> getAllUsersOnlyAllowedWrite() {
    return Arrays.asList("dataWriter");
  }


  private String regexForExpectedExceptions = ".*DATA:READ:.*";

  @Test
  public void cqExecuteNoMethodInvocationWithUsersWithoutCqPermissionsWithPrepopulatedRegionShouldThrowSecurityException()
      throws Exception {
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "select * from /" + regionName + " r where r.id = 0";

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityUnauthorizedUserDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqButExpectException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqExecuteWithInitialResultsWithMethodInvocationWithoutPermissionWithUnpopulatedRegionThrowSecurityException()
      throws Exception {
    String query = "select * from /" + regionName + " r where r.name = 'Beth'";
    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityUnauthorizedUserDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqWithInitialResultsButExpectException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqExecuteWithOutPermissionsWithUnpopulatedRegionShouldNotAllowCq() throws Exception {
    String query = "select * from /" + regionName + " r where r.name = 'Beth'";
    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityUnauthorizedUserDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqButExpectException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqCreatedByAllowedUserButPutDoneByUnallowedReaderShouldStillExecuteWithCqEvent()
      throws Exception {
    assumeTrue(user.equals("dataWriter"));
    String query = "select * from /" + regionName + " r where r.id = 1";
    superUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityUnauthorizedUserDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
    });

    Object[] keys = {"key-0"};
    Object[] values = {new QueryTestObject(1, "Mary")};
    putIntoRegion(specificUserClient, keys, values, regionName);

    superUserClient.invoke(() -> {
      await()
          .untilAsserted(() -> assertEquals(1, cqListener.getNumEvent()));
    });
  }


  protected CqQuery createCq(QueryService queryService, String query, CqListener cqListener)
      throws CqException {
    CqAttributesFactory cqaf = new CqAttributesFactory();
    cqaf.addCqListener(cqListener);
    CqQuery cq = queryService.newCq(query, cqaf.create());
    return cq;
  }

  protected void executeCqButExpectException(CqQuery cq, String user,
      String regexForExpectedException) {
    try {
      cq.execute();
      fail("Expected an exception when executing cq:" + cq.getQueryString() + " with user:" + user);
    } catch (RegionNotFoundException | CqException e) {
      if (!e.getMessage().matches(regexForExpectedException)) {
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getMessage().matches(regexForExpectedException)) {
            return;
          }
          cause = cause.getCause();
        }
        e.printStackTrace();
        fail("Exception thrown did not match:" + regexForExpectedException + ".  Instead was:" + e);
      }
    }
  }

  private void executeCqWithInitialResultsButExpectException(CqQuery cq, String user,
      String regexForExpectedException) {
    try {
      cq.executeWithInitialResults();
      fail("Expected an exception when executing cq:" + cq + " with user:" + user);
    } catch (RegionNotFoundException | CqException e) {
      e.printStackTrace();
      if (!e.getMessage().matches(regexForExpectedException)) {
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getMessage() != null && cause.getMessage().matches(regexForExpectedException)) {
            return;
          }
          cause = cause.getCause();
        }
        e.printStackTrace();
        fail("Exception thrown did not match:" + regexForExpectedException + ".  Instead was:" + e);
      }
    }
  }

  public class CqSecurityTestCqListener implements CqListener {

    private int numEvents = 0;

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents++;
    }

    @Override
    public void onError(CqEvent aCqEvent) {

    }

    public int getNumEvent() {
      return numEvents;
    }


    @Override
    public void close() {

    }
  }
}
