
package com.gemstone.gemfire.security;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;
import security.AuthzCredentialGenerator.ClassCode;

import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePort.Keeper;
import com.gemstone.gemfire.internal.cache.AbstractRegionEntry;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.util.Callable;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.VM;

import security.DummyCredentialGenerator;
import security.XmlAuthzCredentialGenerator;

/**
 * Base class for tests for authorization from client to server. It contains
 * utility functions for the authorization tests from client to server.
 * 
 * @author sumedh
 * @since 5.5
 */
public class ClientAuthorizationTestBase extends DistributedTestCase {

  /** constructor */
  public ClientAuthorizationTestBase(String name) {
    super(name);
  }

  protected static VM server1 = null;

  protected static VM server2 = null;

  protected static VM client1 = null;

  protected static VM client2 = null;

  protected static final String regionName = SecurityTestUtil.regionName;

  protected static final String subregionName = "AuthSubregion";

  protected static final String[] serverExpectedExceptions = {
      "Connection refused",
      AuthenticationRequiredException.class.getName(),
      AuthenticationFailedException.class.getName(),
      NotAuthorizedException.class.getName(),
      GemFireSecurityException.class.getName(),
      RegionDestroyedException.class.getName(),
      ClassNotFoundException.class.getName() };

  protected static final String[] clientExpectedExceptions = {
      AuthenticationFailedException.class.getName(),
      NotAuthorizedException.class.getName(),
      RegionDestroyedException.class.getName() };

  protected static Properties buildProperties(String authenticator,
      String accessor, boolean isAccessorPP, Properties extraAuthProps,
      Properties extraAuthzProps) {

    Properties authProps = new Properties();
    if (authenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, authenticator);
    }
    if (accessor != null) {
      if (isAccessorPP) {
        authProps.setProperty(
            DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME, accessor);
      }
      else {
        authProps.setProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME,
            accessor);
      }
    }
    return SecurityTestUtil.concatProperties(new Properties[] { authProps,
        extraAuthProps, extraAuthzProps });
  }

  public static Integer createCacheServer(Integer locatorPort, Object authProps,
      Object javaProps) {

    if (locatorPort == null) {
      locatorPort = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
    }
    return SecurityTestUtil.createCacheServer((Properties)authProps, javaProps,
        locatorPort, null, null, Boolean.TRUE, new Integer(
            SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createCacheServer(Integer locatorPort, Integer serverPort,
      Object authProps, Object javaProps) {
    if (locatorPort == null) {
      locatorPort = new Integer(AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
    }
    SecurityTestUtil.createCacheServer((Properties)authProps, javaProps,
        locatorPort, null, serverPort, Boolean.TRUE, new Integer(
            SecurityTestUtil.NO_EXCEPTION));
  }

  public static void createCacheClient(Object authInit, Object authProps,
      Object javaProps, Integer[] ports, Integer numConnections,
      Boolean setupDynamicRegionFactory, Integer expectedResult) {

    String authInitStr = (authInit == null ? null : authInit.toString());
    if (authProps == null) {
      authProps = new Properties();
    }
    SecurityTestUtil.createCacheClient(authInitStr, (Properties)authProps,
        (Properties)javaProps, ports, numConnections,
        setupDynamicRegionFactory, expectedResult);
  }

  protected static Region getRegion() {
    return SecurityTestUtil.getCache().getRegion(regionName);
  }

  protected static Region getSubregion() {
    return SecurityTestUtil.getCache().getRegion(
        regionName + '/' + subregionName);
  }

  private static Region createSubregion(Region region) {

    Region subregion = getSubregion();
    if (subregion == null) {
      subregion = region.createSubregion(subregionName, region.getAttributes());
    }
    return subregion;
  }

  protected static String indicesToString(int[] indices) {

    String str = "";
    if (indices != null && indices.length > 0) {
      str += indices[0];
      for (int index = 1; index < indices.length; ++index) {
        str += ",";
        str += indices[index];
      }
    }
    return str;
  }

  private static final int PAUSE = 5 * 1000;
  
  public static void doOp(Byte opCode, int[] indices, Integer flagsI,
      Integer expectedResult) {

    OperationCode op = OperationCode.fromOrdinal(opCode.byteValue());
    boolean operationOmitted = false;
    final int flags = flagsI.intValue();
    Region region = getRegion();
    if ((flags & OpFlags.USE_SUBREGION) > 0) {
      assertNotNull(region);
      Region subregion = null;
      if ((flags & OpFlags.NO_CREATE_SUBREGION) > 0) {
        if ((flags & OpFlags.CHECK_NOREGION) > 0) {
          // Wait for some time for DRF update to come
          SecurityTestUtil.waitForCondition(new Callable() {
            public Object call() throws Exception {
              return Boolean.valueOf(getSubregion() == null);
            }
          });
          subregion = getSubregion();
          assertNull(subregion);
          return;
        }
        else {
          // Wait for some time for DRF update to come
          SecurityTestUtil.waitForCondition(new Callable() {
            public Object call() throws Exception {
              return Boolean.valueOf(getSubregion() != null);
            }
          });
          subregion = getSubregion();
          assertNotNull(subregion);
        }
      }
      else {
        subregion = createSubregion(region);
      }
      assertNotNull(subregion);
      region = subregion;
    }
    else if ((flags & OpFlags.CHECK_NOREGION) > 0) {
      // Wait for some time for region destroy update to come
      SecurityTestUtil.waitForCondition(new Callable() {
        public Object call() throws Exception {
          return Boolean.valueOf(getRegion() == null);
        }
      });
      region = getRegion();
      assertNull(region);
      return;
    }
    else {
      assertNotNull(region);
    }
    final String[] keys = SecurityTestUtil.keys;
    final String[] vals;
    if ((flags & OpFlags.USE_NEWVAL) > 0) {
      vals = SecurityTestUtil.nvalues;
    }
    else {
      vals = SecurityTestUtil.values;
    }
    InterestResultPolicy policy = InterestResultPolicy.KEYS_VALUES;
    if ((flags & OpFlags.REGISTER_POLICY_NONE) > 0) {
      policy = InterestResultPolicy.NONE;
    }
    final int numOps = indices.length;
    getLogWriter().info(
        "Got doOp for op: " + op.toString() + ", numOps: " + numOps
            + ", indices: " + indicesToString(indices) + ", expect: " + expectedResult);
    boolean exceptionOccured = false;
    boolean breakLoop = false;
    if (op.isGet() || 
        op.isContainsKey() || 
        op.isKeySet() || 
        op.isQuery() || 
        op.isExecuteCQ()) {
      try {
        Thread.sleep(PAUSE);
      }
      catch (InterruptedException e) {
        fail("interrupted");
      }
    }
    for (int indexIndex = 0; indexIndex < numOps; ++indexIndex) {
      if (breakLoop) {
        break;
      }
      int index = indices[indexIndex];
      try {
        final Object key = keys[index];
        final Object expectedVal = vals[index];
        if (op.isGet()) {
          Object value = null;
          // this is the case for testing GET_ALL
          if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            List keyList = new ArrayList(numOps);
            Object searchKey;
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              searchKey = keys[keyNum];
              keyList.add(searchKey);
              // local invalidate some keys to force fetch of those keys from
              // server
              if ((flags & OpFlags.CHECK_NOKEY) > 0) {
                AbstractRegionEntry entry = (AbstractRegionEntry)((LocalRegion)region).getRegionEntry(searchKey);
                getLogWriter().info(""+keyNum+": key is " + searchKey + " and entry is " + entry);
                assertFalse(region.containsKey(searchKey));
              }
              else {
                if (keyNumIndex % 2 == 1) {
                  assertTrue(region.containsKey(searchKey));
                  region.localInvalidate(searchKey);
                }
              }
            }
            Map entries = region.getAll(keyList);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              searchKey = keys[keyNum];
              if ((flags & OpFlags.CHECK_FAIL) > 0) {
                assertFalse(entries.containsKey(searchKey));
              }
              else {
                assertTrue(entries.containsKey(searchKey));
                value = entries.get(searchKey);
                assertEquals(vals[keyNum], value);
              }
            }
            break;
          }
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            Callable cond = new Callable() {
              private Region region;

              public Object call() throws Exception {
                Object value = SecurityTestUtil.getLocalValue(region, key);
                return Boolean
                    .valueOf((flags & OpFlags.CHECK_FAIL) > 0 ? !expectedVal
                        .equals(value) : expectedVal.equals(value));
              }

              public Callable init(Region region) {
                this.region = region;
                return this;
              }
            }.init(region);
            SecurityTestUtil.waitForCondition(cond);
            value = SecurityTestUtil.getLocalValue(region, key);
          }
          else if ((flags & OpFlags.USE_GET_ENTRY_IN_TX) > 0) {
            SecurityTestUtil.getCache().getCacheTransactionManager().begin();
            Entry e = region.getEntry(key);
            // Also, check getAll()
            ArrayList a = new ArrayList();
            a.addAll(a);
            region.getAll(a);

            SecurityTestUtil.getCache().getCacheTransactionManager().commit();
            value = e.getValue();
          }
          else {
            if ((flags & OpFlags.CHECK_NOKEY) > 0) {
              assertFalse(region.containsKey(key));
            }
            else {
              assertTrue(region.containsKey(key) || ((LocalRegion)region).getRegionEntry(key).isTombstone());
              region.localInvalidate(key);
            }
            value = region.get(key);
          }
          if ((flags & OpFlags.CHECK_FAIL) > 0) {
            assertFalse(expectedVal.equals(value));
          }
          else {
            assertNotNull(value);
            assertEquals(expectedVal, value);
          }
        }
        else if (op.isPut()) {
          region.put(key, expectedVal);
        }
        else if (op.isPutAll()) {
          HashMap map = new HashMap();
          for (int i=0; i<indices.length; i++) {
            map.put(keys[indices[i]], vals[indices[i]]);
          }
          region.putAll(map);
          breakLoop = true;
        }
        else if (op.isDestroy()) {
          // if (!region.containsKey(key)) {
          // // Since DESTROY will fail unless the value is present
          // // in the local cache, this is a workaround for two cases:
          // // 1. When the operation is supposed to succeed then in
          // // the current AuthzCredentialGenerators the clients having
          // // DESTROY permission also has CREATE/UPDATE permission
          // // so that calling region.put() will work for that case.
          // // 2. When the operation is supposed to fail with
          // // NotAuthorizedException then in the current
          // // AuthzCredentialGenerators the clients not
          // // having DESTROY permission are those with reader role that have
          // // GET permission.
          // //
          // // If either of these assumptions fails, then this has to be
          // // adjusted or reworked accordingly.
          // if ((flags & OpFlags.CHECK_NOTAUTHZ) > 0) {
          // Object value = region.get(key);
          // assertNotNull(value);
          // assertEquals(vals[index], value);
          // }
          // else {
          // region.put(key, vals[index]);
          // }
          // }
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            region.localDestroy(key);
          }
          else {
            region.destroy(key);
          }
        }
        else if (op.isInvalidate()) {
          if (region.containsKey(key)) {
            if ((flags & OpFlags.LOCAL_OP) > 0) {
              region.localInvalidate(key);
            }
            else {
              region.invalidate(key);
            }
          }
        }
        else if (op.isContainsKey()) {
          boolean result;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            result = region.containsKey(key);
          }
          else {
            result = region.containsKeyOnServer(key);
          }
          if ((flags & OpFlags.CHECK_FAIL) > 0) {
            assertFalse(result);
          }
          else {
            assertTrue(result);
          }
        }
        else if (op.isRegisterInterest()) {
          if ((flags & OpFlags.USE_LIST) > 0) {
            breakLoop = true;
            // Register interest list in this case
            List keyList = new ArrayList(numOps);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              keyList.add(keys[keyNum]);
            }
            region.registerInterest(keyList, policy);
          }
          else if ((flags & OpFlags.USE_REGEX) > 0) {
            breakLoop = true;
            region.registerInterestRegex("key[1-" + numOps + ']', policy);
          }
          else if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            region.registerInterest("ALL_KEYS", policy);
          }
          else {
            region.registerInterest(key, policy);
          }
        }
        else if (op.isUnregisterInterest()) {
          if ((flags & OpFlags.USE_LIST) > 0) {
            breakLoop = true;
            // Register interest list in this case
            List keyList = new ArrayList(numOps);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              keyList.add(keys[keyNum]);
            }
            region.unregisterInterest(keyList);
          }
          else if ((flags & OpFlags.USE_REGEX) > 0) {
            breakLoop = true;
            region.unregisterInterestRegex("key[1-" + numOps + ']');
          }
          else if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            region.unregisterInterest("ALL_KEYS");
          }
          else {
            region.unregisterInterest(key);
          }
        }
        else if (op.isKeySet()) {
          breakLoop = true;
          Set keySet;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            keySet = region.keySet();
          }
          else {
            keySet = region.keySetOnServer();
          }
          assertNotNull(keySet);
          if ((flags & OpFlags.CHECK_FAIL) == 0) {
            assertEquals(numOps, keySet.size());
          }
          for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
            int keyNum = indices[keyNumIndex];
            if ((flags & OpFlags.CHECK_FAIL) > 0) {
              assertFalse(keySet.contains(keys[keyNum]));
            }
            else {
              assertTrue(keySet.contains(keys[keyNum]));
            }
          }
        }
        else if (op.isQuery()) {
          breakLoop = true;
          SelectResults queryResults = region.query("SELECT DISTINCT * FROM "
              + region.getFullPath());
          assertNotNull(queryResults);
          Set queryResultSet = queryResults.asSet();
          if ((flags & OpFlags.CHECK_FAIL) == 0) {
            assertEquals(numOps, queryResultSet.size());
          }
          for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
            int keyNum = indices[keyNumIndex];
            if ((flags & OpFlags.CHECK_FAIL) > 0) {
              assertFalse(queryResultSet.contains(vals[keyNum]));
            }
            else {
              assertTrue(queryResultSet.contains(vals[keyNum]));
            }
          }
        }
        else if (op.isExecuteCQ()) {
          breakLoop = true;
          QueryService queryService = SecurityTestUtil.getCache()
              .getQueryService();
          CqQuery cqQuery;
          if ((cqQuery = queryService.getCq("cq1")) == null) {
            CqAttributesFactory cqFact = new CqAttributesFactory();
            cqFact.addCqListener(new AuthzCqListener());
            CqAttributes cqAttrs = cqFact.create();
            cqQuery = queryService.newCq("cq1", "SELECT * FROM "
                + region.getFullPath(), cqAttrs);
          }
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            // Interpret this as testing results using CqListener
            final AuthzCqListener listener = (AuthzCqListener)cqQuery
                .getCqAttributes().getCqListener();
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                if ((flags & OpFlags.CHECK_FAIL) > 0) {
                  return 0 == listener.getNumUpdates();
                }
                else {
                  return numOps == listener.getNumUpdates();
                }
              }
              public String description() {
                return null;
              }
            };
            DistributedTestCase.waitForCriterion(ev, 3 * 1000, 200, true);
            if ((flags & OpFlags.CHECK_FAIL) > 0) {
              assertEquals(0, listener.getNumUpdates());
            }
            else {
              assertEquals(numOps, listener.getNumUpdates());
              listener.checkPuts(vals, indices);
            }
            assertEquals(0, listener.getNumCreates());
            assertEquals(0, listener.getNumDestroys());
            assertEquals(0, listener.getNumOtherOps());
            assertEquals(0, listener.getNumErrors());
          }
          else {
            SelectResults cqResults = cqQuery.executeWithInitialResults();
            assertNotNull(cqResults);
            Set cqResultValues = new HashSet();
            for (Object o : cqResults.asList()) {
              Struct s = (Struct)o;
              cqResultValues.add(s.get("value"));
            }
            Set cqResultSet = cqResults.asSet();
            if ((flags & OpFlags.CHECK_FAIL) == 0) {
              assertEquals(numOps, cqResultSet.size());
            }
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              if ((flags & OpFlags.CHECK_FAIL) > 0) {
                assertFalse(cqResultValues.contains(vals[keyNum]));
              }
              else {
                assertTrue(cqResultValues.contains(vals[keyNum]));
              }
            }
          }
        }
        else if (op.isStopCQ()) {
          breakLoop = true;
          CqQuery cqQuery = SecurityTestUtil.getCache().getQueryService()
              .getCq("cq1");
          ((AuthzCqListener)cqQuery.getCqAttributes().getCqListener()).reset();
          cqQuery.stop();
        }
        else if (op.isCloseCQ()) {
          breakLoop = true;
          CqQuery cqQuery = SecurityTestUtil.getCache().getQueryService()
              .getCq("cq1");
          ((AuthzCqListener)cqQuery.getCqAttributes().getCqListener()).reset();
          cqQuery.close();
        }
        else if (op.isRegionClear()) {
          breakLoop = true;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            region.localClear();
          }
          else {
            region.clear();
          }
        }
        else if (op.isRegionCreate()) {
          breakLoop = true;
          // Region subregion = createSubregion(region);
          // subregion.createRegionOnServer();
          // Create region on server using the DynamicRegionFactory
          // Assume it has been already initialized
          DynamicRegionFactory drf = DynamicRegionFactory.get();
          Region subregion = drf.createDynamicRegion(regionName, subregionName);
          assertEquals('/' + regionName + '/' + subregionName, subregion
              .getFullPath());
        }
        else if (op.isRegionDestroy()) {
          breakLoop = true;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            region.localDestroyRegion();
          }
          else {
            if ((flags & OpFlags.USE_SUBREGION) > 0) {
              try {
                DynamicRegionFactory.get().destroyDynamicRegion(
                    region.getFullPath());
              }
              catch (RegionDestroyedException ex) {
                // harmless to ignore this
                getLogWriter().info(
                    "doOp: sub-region " + region.getFullPath()
                        + " already destroyed");
                operationOmitted = true;
              }
            }
            else {
              region.destroyRegion();
            }
          }
        }
        else {
          fail("doOp: Unhandled operation " + op);
        }
        if (expectedResult.intValue() != SecurityTestUtil.NO_EXCEPTION) {
          if (!operationOmitted && !op.isUnregisterInterest()) {
            fail("Expected an exception while performing operation op =" + op +
                "flags = " + OpFlags.description(flags));
          }
        }
      }
      catch (Exception ex) {
        exceptionOccured = true;
        if ((ex instanceof ServerConnectivityException
            || ex instanceof QueryInvocationTargetException || ex instanceof CqException)
            && (expectedResult.intValue() == SecurityTestUtil.NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "doOp: Got expected NotAuthorizedException when doing operation ["
                  + op + "] with flags " + OpFlags.description(flags) 
                  + ": " + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == SecurityTestUtil.OTHER_EXCEPTION) {
          getLogWriter().info(
              "doOp: Got expected exception when doing operation: "
                  + ex.toString());
          continue;
        }
        else {
          fail("doOp: Got unexpected exception when doing operation. Policy = " 
              + policy + " flags = " + OpFlags.description(flags), ex);
        }
      }
    }
    if (!exceptionOccured && !operationOmitted
        && expectedResult.intValue() != SecurityTestUtil.NO_EXCEPTION) {
      fail("Expected an exception while performing operation: " + op + 
          " flags = " + OpFlags.description(flags));
    }
  }

  protected void executeOpBlock(List opBlock, Integer port1, Integer port2,
      String authInit, Properties extraAuthProps, Properties extraAuthzProps,
      TestCredentialGenerator gen, Random rnd) {

    Iterator opIter = opBlock.iterator();
    while (opIter.hasNext()) {
      // Start client with valid credentials as specified in
      // OperationWithAction
      OperationWithAction currentOp = (OperationWithAction)opIter.next();
      OperationCode opCode = currentOp.getOperationCode();
      int opFlags = currentOp.getFlags();
      int clientNum = currentOp.getClientNum();
      VM clientVM = null;
      boolean useThisVM = false;
      switch (clientNum) {
        case 1:
          clientVM = client1;
          break;
        case 2:
          clientVM = client2;
          break;
        case 3:
          useThisVM = true;
          break;
        default:
          fail("executeOpBlock: Unknown client number " + clientNum);
          break;
      }
      getLogWriter().info(
          "executeOpBlock: performing operation number ["
              + currentOp.getOpNum() + "]: " + currentOp);
      if ((opFlags & OpFlags.USE_OLDCONN) == 0) {
        Properties opCredentials;
        int newRnd = rnd.nextInt(100) + 1;
        String currentRegionName = '/' + regionName;
        if ((opFlags & OpFlags.USE_SUBREGION) > 0) {
          currentRegionName += ('/' + subregionName);
        }
        String credentialsTypeStr;
        OperationCode authOpCode = currentOp.getAuthzOperationCode();
        int[] indices = currentOp.getIndices();
        CredentialGenerator cGen = gen.getCredentialGenerator();
        Properties javaProps = null;
        if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0
            || (opFlags & OpFlags.USE_NOTAUTHZ) > 0) {
          opCredentials = gen.getDisallowedCredentials(
              new OperationCode[] { authOpCode },
              new String[] { currentRegionName }, indices, newRnd);
          credentialsTypeStr = " unauthorized " + authOpCode;
        }
        else {
          opCredentials = gen.getAllowedCredentials(new OperationCode[] {
              opCode, authOpCode }, new String[] { currentRegionName },
              indices, newRnd);
          credentialsTypeStr = " authorized " + authOpCode;
        }
        if (cGen != null) {
          javaProps = cGen.getJavaProperties();
        }
        Properties clientProps = SecurityTestUtil
            .concatProperties(new Properties[] { opCredentials, extraAuthProps,
                extraAuthzProps });
        // Start the client with valid credentials but allowed or disallowed to
        // perform an operation
        getLogWriter().info(
            "executeOpBlock: For client" + clientNum + credentialsTypeStr
                + " credentials: " + opCredentials);
        boolean setupDynamicRegionFactory = (opFlags & OpFlags.ENABLE_DRF) > 0;
        if (useThisVM) {
          createCacheClient(authInit, clientProps, javaProps, new Integer[] {
              port1, port2 }, null, Boolean.valueOf(setupDynamicRegionFactory),
              new Integer(SecurityTestUtil.NO_EXCEPTION));
        }
        else {
          clientVM.invoke(ClientAuthorizationTestBase.class,
              "createCacheClient", new Object[] { authInit, clientProps,
                  javaProps, new Integer[] { port1, port2 }, null,
                  Boolean.valueOf(setupDynamicRegionFactory),
                  new Integer(SecurityTestUtil.NO_EXCEPTION) });
        }
      }
      int expectedResult;
      if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0) {
        expectedResult = SecurityTestUtil.NOTAUTHZ_EXCEPTION;
      }
      else if ((opFlags & OpFlags.CHECK_EXCEPTION) > 0) {
        expectedResult = SecurityTestUtil.OTHER_EXCEPTION;
      }
      else {
        expectedResult = SecurityTestUtil.NO_EXCEPTION;
      }

      // Perform the operation from selected client
      if (useThisVM) {
        doOp(new Byte(opCode.toOrdinal()), currentOp.getIndices(), new Integer(
            opFlags), new Integer(expectedResult));
      }
      else {
        clientVM.invoke(ClientAuthorizationTestBase.class, "doOp",
            new Object[] { new Byte(opCode.toOrdinal()),
                currentOp.getIndices(), new Integer(opFlags),
                new Integer(expectedResult) });
      }
    }
  }

  protected AuthzCredentialGenerator getXmlAuthzGenerator(){
    AuthzCredentialGenerator authzGen = new XmlAuthzCredentialGenerator();
    CredentialGenerator cGen = new DummyCredentialGenerator();
    cGen.init();
    authzGen.init(cGen);
    return authzGen;
  }

  protected List getDummyGeneratorCombos() {
    List generators = new ArrayList();
    Iterator authzCodeIter = AuthzCredentialGenerator.ClassCode.getAll()
            .iterator();
    while (authzCodeIter.hasNext()) {
      ClassCode authzClassCode = (ClassCode) authzCodeIter.next();
      AuthzCredentialGenerator authzGen = AuthzCredentialGenerator
              .create(authzClassCode);
      if (authzGen != null) {
        CredentialGenerator cGen = new DummyCredentialGenerator();
        cGen.init();
        if (authzGen.init(cGen)) {
          generators.add(authzGen);
        }
      }
    }

    assertTrue(generators.size() > 0);
    return generators;
  }


  protected void runOpsWithFailover(OperationWithAction[] opCodes,
      String testName) {
      AuthzCredentialGenerator gen = getXmlAuthzGenerator();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties javaProps = cGen.getJavaProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();
      TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

      getLogWriter().info(testName + ": Using authinit: " + authInit);
      getLogWriter().info(testName + ": Using authenticator: " + authenticator);
      getLogWriter().info(testName + ": Using accessor: " + accessor);

      // Start servers with all required properties
      Properties serverProps = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      // Get ports for the servers
      Keeper port1Keeper = AvailablePort.getRandomAvailablePortKeeper(AvailablePort.SOCKET);
      Keeper port2Keeper = AvailablePort.getRandomAvailablePortKeeper(AvailablePort.SOCKET);
      int port1 = port1Keeper.getPort();
      int port2 = port2Keeper.getPort();

      // Perform all the ops on the clients
      List opBlock = new ArrayList();
      Random rnd = new Random();
      for (int opNum = 0; opNum < opCodes.length; ++opNum) {
        // Start client with valid credentials as specified in
        // OperationWithAction
        OperationWithAction currentOp = opCodes[opNum];
        if (currentOp.equals(OperationWithAction.OPBLOCK_END)
            || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
          // End of current operation block; execute all the operations
          // on the servers with/without failover
          if (opBlock.size() > 0) {
            port1Keeper.release();
            // Start the first server and execute the operation block
            server1.invoke(ClientAuthorizationTestBase.class,
                "createCacheServer", new Object[] {
                    SecurityTestUtil.getLocatorPort(), port1, serverProps,
                    javaProps });
            server2.invoke(SecurityTestUtil.class, "closeCache");
            executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                extraAuthzProps, tgen, rnd);
            if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
              // Failover to the second server and run the block again
              port2Keeper.release();
              server2.invoke(ClientAuthorizationTestBase.class,
                  "createCacheServer", new Object[] {
                      SecurityTestUtil.getLocatorPort(), port2, serverProps,
                      javaProps });
              server1.invoke(SecurityTestUtil.class, "closeCache");
              executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps,
                  extraAuthzProps, tgen, rnd);
            }
            opBlock.clear();
          }
        }
        else {
          currentOp.setOpNum(opNum);
          opBlock.add(currentOp);
        }
      }
  }

  /**
   * Implements the {@link CqListener} interface and counts the number of
   * different operations and also queues up the received updates to precise
   * checking of each update.
   * 
   * @author sumedh
   * @since 5.5
   */
  public static class AuthzCqListener implements CqListener {

    private List eventList;

    private int numCreates;

    private int numUpdates;

    private int numDestroys;

    private int numOtherOps;

    private int numErrors;

    public AuthzCqListener() {
      this.eventList = new ArrayList();
      reset();
    }

    public void reset() {
      this.eventList.clear();
      this.numCreates = 0;
      this.numUpdates = 0;
      this.numErrors = 0;
    }

    public void onEvent(CqEvent aCqEvent) {
      Operation op = aCqEvent.getBaseOperation();
      if (op.isCreate()) {
        ++this.numCreates;
      }
      else if (op.isUpdate()) {
        ++this.numUpdates;
      }
      else if (op.isDestroy()) {
        ++this.numDestroys;
      }
      else {
        ++this.numOtherOps;
      }
      eventList.add(aCqEvent);
    }

    public void onError(CqEvent aCqEvent) {
      ++this.numErrors;
    }

    public void close() {
      this.eventList.clear();
    }

    public int getNumCreates() {
      return this.numCreates;
    }

    public int getNumUpdates() {
      return this.numUpdates;
    }

    public int getNumDestroys() {
      return this.numDestroys;
    }

    public int getNumOtherOps() {
      return this.numOtherOps;
    }

    public int getNumErrors() {
      return this.numErrors;
    }

    public void checkPuts(String[] vals, int[] indices) {
      for (int indexIndex = 0; indexIndex < indices.length; ++indexIndex) {
        int index = indices[indexIndex];
        Iterator eventIter = this.eventList.iterator();
        boolean foundKey = false;
        while (eventIter.hasNext()) {
          CqEvent event = (CqEvent)eventIter.next();
          if (SecurityTestUtil.keys[index].equals(event.getKey())) {
            assertEquals(vals[index], event.getNewValue());
            foundKey = true;
            break;
          }
        }
        assertTrue(foundKey);
      }
    }
  }

  /**
   * This class specifies flags that can be used to alter the behaviour of
   * operations being performed by the <code>doOp</code> function.
   * 
   * @author sumedh
   * @since 5.5
   */
  public static class OpFlags {

    /**
     * Default behaviour.
     */
    public static final int NONE = 0x0;

    /**
     * Check that the operation should fail.
     */
    public static final int CHECK_FAIL = 0x1;

    /**
     * Check that the operation should throw <code>NotAuthorizedException</code>.
     */
    public static final int CHECK_NOTAUTHZ = 0x2;

    /**
     * Check that the region should not be available.
     */
    public static final int CHECK_NOREGION = 0x4;

    /**
     * Check that the operation should throw an exception other than the
     * <code>NotAuthorizedException</code>.
     */
    public static final int CHECK_EXCEPTION = 0x8;

    /**
     * Check for nvalues[] instead of values[].
     */
    public static final int USE_NEWVAL = 0x10;

    /**
     * Register all keys. For GET operations indicates using getAll().
     */
    public static final int USE_ALL_KEYS = 0x20;

    /**
     * Register a regular expression.
     */
    public static final int USE_REGEX = 0x40;

    /**
     * Register a list of keys.
     */
    public static final int USE_LIST = 0x80;

    /**
     * Perform the local version of the operation.
     */
    public static final int LOCAL_OP = 0x100;

    /**
     * Check that the key for the operation should not be present.
     */
    public static final int CHECK_NOKEY = 0x200;

    /**
     * Use the sub-region for performing the operation.
     */
    public static final int USE_SUBREGION = 0x400;

    /**
     * Do not try to create the sub-region.
     */
    public static final int NO_CREATE_SUBREGION = 0x800;

    /**
     * Do not re-connect using new credentials rather use the previous
     * connection.
     */
    public static final int USE_OLDCONN = 0x1000;

    /**
     * Do the connection with unauthorized credentials but do not check that the
     * operation throws <code>NotAuthorizedException</code>.
     */
    public static final int USE_NOTAUTHZ = 0x2000;

    /**
     * Enable {@link DynamicRegionFactory} on the client.
     */
    public static final int ENABLE_DRF = 0x4000;

    /**
     * Use the {@link InterestResultPolicy#NONE} for register interest.
     */
    public static final int REGISTER_POLICY_NONE = 0x8000;
    
    /**
     * Use the {@link LocalRegion#getEntry} under transaction.
     */
    public static final int USE_GET_ENTRY_IN_TX = 0x10000;
    
    static public String description(int f) {
      StringBuffer sb = new StringBuffer();
      sb.append("[");
      if ((f & CHECK_FAIL) != 0) {
        sb.append("CHECK_FAIL,");
      }
      if ((f & CHECK_NOTAUTHZ) != 0) {
        sb.append("CHECK_NOTAUTHZ,");
      }
      if ((f & CHECK_NOREGION) != 0) {
        sb.append("CHECK_NOREGION,");
      }
      if ((f & CHECK_EXCEPTION) != 0) {
        sb.append("CHECK_EXCEPTION,");
      }
      if ((f & USE_NEWVAL) != 0) {
        sb.append("USE_NEWVAL,");
      }
      if ((f & USE_ALL_KEYS) != 0) {
        sb.append("USE_ALL_KEYS,");
      }
      if ((f & USE_REGEX) != 0) {
        sb.append("USE_REGEX,");
      }
      if ((f & USE_LIST) != 0) {
        sb.append("USE_LIST,");
      }
      if ((f & LOCAL_OP) != 0) {
        sb.append("LOCAL_OP,");
      }
      if ((f & CHECK_NOKEY) != 0) {
        sb.append("CHECK_NOKEY,");
      }
      if ((f & USE_SUBREGION) != 0) {
        sb.append("USE_SUBREGION,");
      }
      if ((f & NO_CREATE_SUBREGION) != 0) {
        sb.append("NO_CREATE_SUBREGION,");
      }
      if ((f & USE_OLDCONN) != 0) {
        sb.append("USE_OLDCONN,");
      }
      if ((f & USE_NOTAUTHZ) != 0) {
        sb.append("USE_NOTAUTHZ");
      }
      if ((f & ENABLE_DRF) != 0) {
        sb.append("ENABLE_DRF,");
      }
      if ((f & REGISTER_POLICY_NONE) != 0) {
        sb.append("REGISTER_POLICY_NONE,");
      }
      sb.append("]");
      return sb.toString();
    }
  }

  /**
   * This class encapsulates an {@link OperationCode} with associated flags, the
   * client to perform the operation, and the number of operations to perform.
   * 
   * @author sumedh
   * @since 5.5
   */
  public static class OperationWithAction {

    /**
     * The operation to be performed.
     */
    private OperationCode opCode;

    /**
     * The operation for which authorized or unauthorized credentials have to be
     * generated. This is the same as {@link #opCode} when not specified.
     */
    private OperationCode authzOpCode;

    /**
     * The client number on which the operation has to be performed.
     */
    private int clientNum;

    /**
     * Bitwise or'd {@link OpFlags} integer to change/specify the behaviour of
     * the operations.
     */
    private int flags;

    /**
     * Indices of the keys array to be used for operations.
     */
    private int[] indices;

    /**
     * An index for the operation used for logging.
     */
    private int opNum;

    /**
     * Indicates end of an operation block which can be used for testing with
     * failover
     */
    public static final OperationWithAction OPBLOCK_END = new OperationWithAction(
        null, 4);

    /**
     * Indicates end of an operation block which should not be used for testing
     * with failover
     */
    public static final OperationWithAction OPBLOCK_NO_FAILOVER = new OperationWithAction(
        null, 5);

    private void setIndices(int numOps) {

      this.indices = new int[numOps];
      for (int index = 0; index < numOps; ++index) {
        this.indices[index] = index;
      }
    }

    public OperationWithAction(OperationCode opCode) {

      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = 1;
      this.flags = OpFlags.NONE;
      setIndices(4);
      this.opNum = 0;
    }

    public OperationWithAction(OperationCode opCode, int clientNum) {

      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = clientNum;
      this.flags = OpFlags.NONE;
      setIndices(4);
      this.opNum = 0;
    }

    public OperationWithAction(OperationCode opCode, int clientNum, int flags,
        int numOps) {

      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = clientNum;
      this.flags = flags;
      setIndices(numOps);
      this.opNum = 0;
    }

    public OperationWithAction(OperationCode opCode,
        OperationCode deniedOpCode, int clientNum, int flags, int numOps) {

      this.opCode = opCode;
      this.authzOpCode = deniedOpCode;
      this.clientNum = clientNum;
      this.flags = flags;
      setIndices(numOps);
      this.opNum = 0;
    }

    public OperationWithAction(OperationCode opCode, int clientNum, int flags,
        int[] indices) {

      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = clientNum;
      this.flags = flags;
      this.indices = indices;
      this.opNum = 0;
    }

    public OperationWithAction(OperationCode opCode,
        OperationCode deniedOpCode, int clientNum, int flags, int[] indices) {

      this.opCode = opCode;
      this.authzOpCode = deniedOpCode;
      this.clientNum = clientNum;
      this.flags = flags;
      this.indices = indices;
      this.opNum = 0;
    }

    public OperationCode getOperationCode() {
      return this.opCode;
    }

    public OperationCode getAuthzOperationCode() {
      return this.authzOpCode;
    }

    public int getClientNum() {
      return this.clientNum;
    }

    public int getFlags() {
      return this.flags;
    }

    public int[] getIndices() {
      return this.indices;
    }

    public int getOpNum() {
      return this.opNum;
    }

    public void setOpNum(int opNum) {
      this.opNum = opNum;
    }

    public String toString() {
      return "opCode:" + this.opCode + ",authOpCode:" + this.authzOpCode
          + ",clientNum:" + this.clientNum + ",flags:" + this.flags
          + ",numOps:" + this.indices.length + ",indices:"
          + indicesToString(this.indices);
    }
  }

  /**
   * Simple interface to generate credentials with authorization based on key
   * indices also. This is utilized by the post-operation authorization tests
   * where authorization is based on key indices.
   * 
   * @author sumedh
   * @since 5.5
   */
  public interface TestCredentialGenerator {

    /**
     * Get allowed credentials for the given set of operations in the given
     * regions and indices of keys in the <code>keys</code> array
     */
    public Properties getAllowedCredentials(OperationCode[] opCodes,
        String[] regionNames, int[] keyIndices, int num);

    /**
     * Get disallowed credentials for the given set of operations in the given
     * regions and indices of keys in the <code>keys</code> array
     */
    public Properties getDisallowedCredentials(OperationCode[] opCodes,
        String[] regionNames, int[] keyIndices, int num);

    /**
     * Get the {@link CredentialGenerator} if any.
     */
    public CredentialGenerator getCredentialGenerator();
  }

  /**
   * Contains a {@link AuthzCredentialGenerator} and implements the
   * {@link TestCredentialGenerator} interface.
   * 
   * @author sumedh
   * @since 5.5
   */
  protected static class TestAuthzCredentialGenerator implements
      TestCredentialGenerator {

    private AuthzCredentialGenerator authzGen;

    public TestAuthzCredentialGenerator(AuthzCredentialGenerator authzGen) {
      this.authzGen = authzGen;
    }

    public Properties getAllowedCredentials(OperationCode[] opCodes,
        String[] regionNames, int[] keyIndices, int num) {

      return this.authzGen.getAllowedCredentials(opCodes, regionNames, num);
    }

    public Properties getDisallowedCredentials(OperationCode[] opCodes,
        String[] regionNames, int[] keyIndices, int num) {

      return this.authzGen.getDisallowedCredentials(opCodes, regionNames, num);
    }

    public CredentialGenerator getCredentialGenerator() {

      return authzGen.getCredentialGenerator();
    }
  }

}
