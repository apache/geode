/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.AvailablePort.*;
import static org.apache.geode.security.SecurityTestUtils.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.apache.geode.test.dunit.Wait.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.internal.AvailablePort.*;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.security.generator.AuthzCredentialGenerator;
import org.apache.geode.security.generator.AuthzCredentialGenerator.ClassCode;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.security.generator.XmlAuthzCredentialGenerator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.VersionManager;

/**
 * Base class for tests for authorization from client to server. It contains utility functions for
 * the authorization tests from client to server.
 *
 * @since GemFire 5.5
 */
public abstract class ClientAuthorizationTestCase extends JUnit4DistributedTestCase {

  private static final int PAUSE = 5 * 1000;

  protected static VM server1 = null;
  protected static VM server2 = null;
  protected static VM client1 = null;
  protected static VM client2 = null;

  public String clientVersion = VersionManager.CURRENT_VERSION;

  protected static final String regionName = REGION_NAME; // TODO: remove
  protected static final String SUBREGION_NAME = "AuthSubregion";

  private static final String[] serverIgnoredExceptions =
      {"Connection refused", AuthenticationRequiredException.class.getName(),
          AuthenticationFailedException.class.getName(), NotAuthorizedException.class.getName(),
          GemFireSecurityException.class.getName(), RegionDestroyedException.class.getName(),
          ClassNotFoundException.class.getName()};

  private static final String[] clientIgnoredExceptions =
      {AuthenticationFailedException.class.getName(), NotAuthorizedException.class.getName(),
          RegionDestroyedException.class.getName()};

  @Override
  public final void preSetUp() throws Exception {}

  @Override
  public final void postSetUp() throws Exception {
    preSetUpClientAuthorizationTestBase();
    setUpClientAuthorizationTestBase();
    postSetUpClientAuthorizationTestBase();
  }

  private void setUpClientAuthorizationTestBase() throws Exception {
    Host host = getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server1.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = false);
    server2.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = false);
    if (VersionManager.isCurrentVersion(clientVersion)) {
      client1 = host.getVM(2);
      client2 = host.getVM(3);
    } else {
      client1 = host.getVM(clientVersion, 2);
      client2 = host.getVM(clientVersion, 3);
    }
    setUpIgnoredExceptions();
  }

  private void setUpIgnoredExceptions() {
    Set<String> serverExceptions = new HashSet<>();
    serverExceptions.addAll(Arrays.asList(serverIgnoredExceptions()));
    if (serverExceptions.isEmpty()) {
      serverExceptions.addAll(Arrays.asList(serverIgnoredExceptions));
    }

    String[] serverExceptionsArray = serverExceptions.toArray(new String[serverExceptions.size()]);
    server1.invoke(() -> registerExpectedExceptions(serverExceptionsArray));
    server2.invoke(() -> registerExpectedExceptions(serverExceptionsArray));

    Set<String> clientExceptions = new HashSet<>();
    clientExceptions.addAll(Arrays.asList(clientIgnoredExceptions()));
    if (clientExceptions.isEmpty()) {
      clientExceptions.addAll(Arrays.asList(clientIgnoredExceptions));
    }

    String[] clientExceptionsArray = serverExceptions.toArray(new String[clientExceptions.size()]);
    client2.invoke(() -> registerExpectedExceptions(clientExceptionsArray));
    registerExpectedExceptions(clientExceptionsArray);
  }

  protected String[] serverIgnoredExceptions() {
    return new String[] {};
  }

  protected String[] clientIgnoredExceptions() {
    return new String[] {};
  }

  protected void preSetUpClientAuthorizationTestBase() throws Exception {}

  protected void postSetUpClientAuthorizationTestBase() throws Exception {}

  @Override
  public final void preTearDown() throws Exception {
    preTearDownClientAuthorizationTestBase();
    tearDownClientAuthorizationTestBase();
    postTearDownClientAuthorizationTestBase();
  }

  @Override
  public final void postTearDown() throws Exception {}

  private final void tearDownClientAuthorizationTestBase() throws Exception {
    server1.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = true);
    server2.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = true);
    client1.invoke(() -> closeCache());
    client2.invoke(() -> closeCache());
    server1.invoke(() -> closeCache());
    server2.invoke(() -> closeCache());
  }

  protected void preTearDownClientAuthorizationTestBase() throws Exception {}

  protected void postTearDownClientAuthorizationTestBase() throws Exception {}

  protected static Properties buildProperties(final String authenticator, final String accessor,
      final boolean isAccessorPP, final Properties extraAuthProps,
      final Properties extraAuthzProps) {
    Properties authProps = new Properties();
    if (authenticator != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR, authenticator);
    }
    if (accessor != null) {
      if (isAccessorPP) {
        authProps.setProperty(SECURITY_CLIENT_ACCESSOR_PP, accessor);
      } else {
        authProps.setProperty(SECURITY_CLIENT_ACCESSOR, accessor);
      }
    }
    return concatProperties(new Properties[] {authProps, extraAuthProps, extraAuthzProps});
  }

  protected static Integer createCacheServer(int locatorPort, final Properties authProps,
      final Properties javaProps) {
    if (locatorPort == 0) {
      locatorPort = getRandomAvailablePort(SOCKET);
    }
    return SecurityTestUtils.createCacheServer(authProps, javaProps, locatorPort, null, 0, true,
        NO_EXCEPTION);
  }

  protected static int createCacheServer(int locatorPort, final int serverPort,
      final Properties authProps, final Properties javaProps) {
    if (locatorPort == 0) {
      locatorPort = getRandomAvailablePort(SOCKET);
    }
    return SecurityTestUtils.createCacheServer(authProps, javaProps, locatorPort, null, serverPort,
        true, NO_EXCEPTION);
  }

  protected static Region getRegion() {
    return getCache().getRegion(regionName);
  }

  protected static Region getSubregion() {
    return getCache().getRegion(regionName + '/' + SUBREGION_NAME);
  }

  private static Region createSubregion(final Region region) {
    Region subregion = getSubregion();
    if (subregion == null) {
      subregion = region.createSubregion(SUBREGION_NAME, region.getAttributes());
    }
    return subregion;
  }

  protected static String indicesToString(final int[] indices) {
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

  protected static void doOp(OperationCode op, final int[] indices, final int flagsI,
      final int expectedResult) throws InterruptedException {
    boolean operationOmitted = false;
    final int flags = flagsI;
    Region region = getRegion();

    if ((flags & OpFlags.USE_SUBREGION) > 0) {
      assertNotNull(region);
      Region subregion = null;

      if ((flags & OpFlags.NO_CREATE_SUBREGION) > 0) {
        if ((flags & OpFlags.CHECK_NOREGION) > 0) {
          // Wait for some time for DRF update to come
          waitForCondition(() -> getSubregion() == null);
          subregion = getSubregion();
          assertNull(subregion);
          return;

        } else {
          // Wait for some time for DRF update to come
          waitForCondition(() -> getSubregion() != null);
          subregion = getSubregion();
          assertNotNull(subregion);
        }

      } else {
        subregion = createSubregion(region);
      }

      assertNotNull(subregion);
      region = subregion;

    } else if ((flags & OpFlags.CHECK_NOREGION) > 0) {
      // Wait for some time for region destroy update to come
      waitForCondition(() -> getRegion() == null);
      region = getRegion();
      assertNull(region);
      return;

    } else {
      assertNotNull(region);
    }

    final String[] keys = KEYS;
    final String[] vals;
    if ((flags & OpFlags.USE_NEWVAL) > 0) {
      vals = NVALUES;
    } else {
      vals = VALUES;
    }

    InterestResultPolicy policy = InterestResultPolicy.KEYS_VALUES;
    if ((flags & OpFlags.REGISTER_POLICY_NONE) > 0) {
      policy = InterestResultPolicy.NONE;
    }

    final int numOps = indices.length;
    System.out.println("Got doOp for op: " + op.toString() + ", numOps: " + numOps + ", indices: "
        + indicesToString(indices) + ", expect: " + expectedResult + " flags: "
        + OpFlags.description(flags));
    boolean exceptionOccurred = false;
    boolean breakLoop = false;

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

              // local invalidate some KEYS to force fetch of those KEYS from server
              if ((flags & OpFlags.CHECK_NOKEY) > 0) {
                AbstractRegionEntry entry =
                    (AbstractRegionEntry) ((LocalRegion) region).getRegionEntry(searchKey);
                System.out
                    .println("" + keyNum + ": key is " + searchKey + " and entry is " + entry);
                assertFalse(region.containsKey(searchKey));
              } else {
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
              } else {
                assertTrue(entries.containsKey(searchKey));
                value = entries.get(searchKey);
                assertEquals(vals[keyNum], value);
              }
            }

            break;
          }

          if ((flags & OpFlags.LOCAL_OP) > 0) {
            Callable<Boolean> condition = new Callable<Boolean>() {
              private Region region;

              @Override
              public Boolean call() throws Exception {
                Object value = getLocalValue(region, key);
                return (flags & OpFlags.CHECK_FAIL) > 0 ? !expectedVal.equals(value)
                    : expectedVal.equals(value);
              }

              public Callable<Boolean> init(Region region) {
                this.region = region;
                return this;
              }
            }.init(region);
            waitForCondition(condition);

            value = getLocalValue(region, key);

          } else if ((flags & OpFlags.USE_GET_ENTRY_IN_TX) > 0) {
            getCache().getCacheTransactionManager().begin();
            Entry e = region.getEntry(key);

            // Also, check getAll()
            ArrayList a = new ArrayList();
            a.addAll(a);
            region.getAll(a);

            getCache().getCacheTransactionManager().commit();
            value = e.getValue();

          } else {
            if ((flags & OpFlags.CHECK_NOKEY) > 0) {
              assertFalse(region.containsKey(key));
            } else {
              assertTrue(region.containsKey(key)
                  || ((LocalRegion) region).getRegionEntry(key).isTombstone());
              region.localInvalidate(key);
            }
            value = region.get(key);
          }

          if ((flags & OpFlags.CHECK_FAIL) > 0) {
            assertFalse(expectedVal.equals(value));
          } else {
            assertNotNull(value);
            assertEquals(expectedVal, value);
          }

        } else if (op.isPut()) {
          region.put(key, expectedVal);

        } else if (op.isPutAll()) {
          HashMap map = new HashMap();
          for (int i = 0; i < indices.length; i++) {
            map.put(keys[indices[i]], vals[indices[i]]);
          }
          region.putAll(map);
          breakLoop = true;

        } else if (op.isDestroy()) {
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
          // assertIndexDetailsEquals(vals[index], value);
          // }
          // else {
          // region.put(key, vals[index]);
          // }
          // }
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            region.localDestroy(key);
          } else {
            region.destroy(key);
          }

        } else if (op.isInvalidate()) {
          if (region.containsKey(key)) {
            if ((flags & OpFlags.LOCAL_OP) > 0) {
              region.localInvalidate(key);
            } else {
              region.invalidate(key);
            }
          }

        } else if (op.isContainsKey()) {
          boolean result;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            result = region.containsKey(key);
          } else {
            result = region.containsKeyOnServer(key);
          }
          if ((flags & OpFlags.CHECK_FAIL) > 0) {
            assertFalse(result);
          } else {
            assertTrue(result);
          }

        } else if (op.isRegisterInterest()) {
          if ((flags & OpFlags.USE_LIST) > 0) {
            breakLoop = true;
            // Register interest list in this case
            List keyList = new ArrayList(numOps);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              keyList.add(keys[keyNum]);
            }
            region.registerInterest(keyList, policy);

          } else if ((flags & OpFlags.USE_REGEX) > 0) {
            breakLoop = true;
            region.registerInterestRegex("key[1-" + numOps + ']', policy);

          } else if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            region.registerInterest("ALL_KEYS", policy);

          } else {
            region.registerInterest(key, policy);
          }

        } else if (op.isUnregisterInterest()) {
          if ((flags & OpFlags.USE_LIST) > 0) {
            breakLoop = true;
            // Register interest list in this case
            List keyList = new ArrayList(numOps);
            for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
              int keyNum = indices[keyNumIndex];
              keyList.add(keys[keyNum]);
            }
            region.unregisterInterest(keyList);

          } else if ((flags & OpFlags.USE_REGEX) > 0) {
            breakLoop = true;
            region.unregisterInterestRegex("key[1-" + numOps + ']');

          } else if ((flags & OpFlags.USE_ALL_KEYS) > 0) {
            breakLoop = true;
            region.unregisterInterest("ALL_KEYS");

          } else {
            region.unregisterInterest(key);
          }

        } else if (op.isKeySet()) {
          breakLoop = true;
          Set keySet;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            keySet = region.keySet();
          } else {
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
            } else {
              assertTrue(keySet.contains(keys[keyNum]));
            }
          }

        } else if (op.isQuery()) {
          breakLoop = true;
          SelectResults queryResults =
              region.query("SELECT DISTINCT * FROM " + region.getFullPath());
          assertNotNull(queryResults);
          Set queryResultSet = queryResults.asSet();
          if ((flags & OpFlags.CHECK_FAIL) == 0) {
            assertEquals(numOps, queryResultSet.size());
          }
          for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex) {
            int keyNum = indices[keyNumIndex];
            if ((flags & OpFlags.CHECK_FAIL) > 0) {
              assertFalse(queryResultSet.contains(vals[keyNum]));
            } else {
              assertTrue(queryResultSet.contains(vals[keyNum]));
            }
          }

        } else if (op.isExecuteCQ()) {
          breakLoop = true;
          QueryService queryService = getCache().getQueryService();
          CqQuery cqQuery;
          if ((cqQuery = queryService.getCq("cq1")) == null) {
            CqAttributesFactory cqFact = new CqAttributesFactory();
            cqFact.addCqListener(new AuthzCqListener());
            CqAttributes cqAttrs = cqFact.create();
            cqQuery = queryService.newCq("cq1", "SELECT * FROM " + region.getFullPath(), cqAttrs);
          }

          if ((flags & OpFlags.LOCAL_OP) > 0) {
            // Interpret this as testing results using CqListener
            final AuthzCqListener listener =
                (AuthzCqListener) cqQuery.getCqAttributes().getCqListener();
            WaitCriterion ev = new WaitCriterion() {
              @Override
              public boolean done() {
                if ((flags & OpFlags.CHECK_FAIL) > 0) {
                  return 0 == listener.getNumUpdates();
                } else {
                  return numOps == listener.getNumUpdates();
                }
              }

              @Override
              public String description() {
                return null;
              }
            };
            waitForCriterion(ev, 3 * 1000, 200, true);

            if ((flags & OpFlags.CHECK_FAIL) > 0) {
              assertEquals(0, listener.getNumUpdates());
            } else {
              assertEquals(numOps, listener.getNumUpdates());
              listener.checkPuts(vals, indices);
            }

            assertEquals(0, listener.getNumCreates());
            assertEquals(0, listener.getNumDestroys());
            assertEquals(0, listener.getNumOtherOps());
            assertEquals(0, listener.getNumErrors());

          } else {
            SelectResults cqResults = cqQuery.executeWithInitialResults();
            assertNotNull(cqResults);
            Set cqResultValues = new HashSet();
            for (Object o : cqResults.asList()) {
              Struct s = (Struct) o;
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
              } else {
                assertTrue(cqResultValues.contains(vals[keyNum]));
              }
            }
          }

        } else if (op.isStopCQ()) {
          breakLoop = true;
          CqQuery cqQuery = getCache().getQueryService().getCq("cq1");
          ((AuthzCqListener) cqQuery.getCqAttributes().getCqListener()).reset();
          cqQuery.stop();

        } else if (op.isCloseCQ()) {
          breakLoop = true;
          CqQuery cqQuery = getCache().getQueryService().getCq("cq1");
          ((AuthzCqListener) cqQuery.getCqAttributes().getCqListener()).reset();
          cqQuery.close();

        } else if (op.isRegionClear()) {
          breakLoop = true;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            region.localClear();
          } else {
            region.clear();
          }

        } else if (op.isRegionCreate()) {
          breakLoop = true;
          // Region subregion = createSubregion(region);
          // subregion.createRegionOnServer();
          // Create region on server using the DynamicRegionFactory
          // Assume it has been already initialized
          DynamicRegionFactory drf = DynamicRegionFactory.get();
          Region subregion = drf.createDynamicRegion(regionName, SUBREGION_NAME);
          assertEquals('/' + regionName + '/' + SUBREGION_NAME, subregion.getFullPath());

        } else if (op.isRegionDestroy()) {
          breakLoop = true;
          if ((flags & OpFlags.LOCAL_OP) > 0) {
            region.localDestroyRegion();

          } else {
            if ((flags & OpFlags.USE_SUBREGION) > 0) {
              try {
                DynamicRegionFactory.get().destroyDynamicRegion(region.getFullPath());
              } catch (RegionDestroyedException ex) {
                // harmless to ignore this
                System.out
                    .println("doOp: sub-region " + region.getFullPath() + " already destroyed");
                operationOmitted = true;
              }
            } else {
              region.destroyRegion();
            }
          }

        } else {
          fail("doOp: Unhandled operation " + op);
        }

        if (expectedResult != NO_EXCEPTION) {
          if (!operationOmitted && !op.isUnregisterInterest()) {
            fail("Expected an exception while performing operation op =" + op + "flags = "
                + OpFlags.description(flags));
          }
        }

      } catch (Exception ex) {
        exceptionOccurred = true;
        if ((ex instanceof ServerConnectivityException
            || ex instanceof QueryInvocationTargetException || ex instanceof CqException)
            && (expectedResult == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          System.out.println("doOp: Got expected NotAuthorizedException when doing operation [" + op
              + "] with flags " + OpFlags.description(flags) + ": " + ex.getCause());
          continue;
        } else if (expectedResult == OTHER_EXCEPTION) {
          System.out.println("doOp: Got expected exception when doing operation: " + ex.toString());
          continue;
        } else {
          fail("doOp: Got unexpected exception when doing operation. Policy = " + policy
              + " flags = " + OpFlags.description(flags), ex);
        }
      }
    }
    if (!exceptionOccurred && !operationOmitted && expectedResult != NO_EXCEPTION) {
      fail("Expected an exception while performing operation: " + op + " flags = "
          + OpFlags.description(flags));
    }
  }

  protected void executeOpBlock(final List<OperationWithAction> opBlock, final int port1,
      final int port2, final String authInit, final Properties extraAuthProps,
      final Properties extraAuthzProps, final TestCredentialGenerator credentialGenerator,
      final Random random) throws InterruptedException {
    for (Iterator<OperationWithAction> opIter = opBlock.iterator(); opIter.hasNext();) {
      // Start client with valid credentials as specified in OperationWithAction
      OperationWithAction currentOp = opIter.next();
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

      System.out.println("executeOpBlock: performing operation number [" + currentOp.getOpNum()
          + "]: " + currentOp);
      if ((opFlags & OpFlags.USE_OLDCONN) == 0) {
        Properties opCredentials;
        int newRnd = random.nextInt(100) + 1;
        String currentRegionName = '/' + regionName;
        if ((opFlags & OpFlags.USE_SUBREGION) > 0) {
          currentRegionName += ('/' + SUBREGION_NAME);
        }

        String credentialsTypeStr;
        OperationCode authOpCode = currentOp.getAuthzOperationCode();
        int[] indices = currentOp.getIndices();
        CredentialGenerator cGen = credentialGenerator.getCredentialGenerator();
        final Properties javaProps = cGen == null ? null : cGen.getJavaProperties();

        if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0 || (opFlags & OpFlags.USE_NOTAUTHZ) > 0) {
          opCredentials = credentialGenerator.getDisallowedCredentials(
              new OperationCode[] {authOpCode}, new String[] {currentRegionName}, indices, newRnd);
          credentialsTypeStr = " unauthorized " + authOpCode;
        } else {
          opCredentials =
              credentialGenerator.getAllowedCredentials(new OperationCode[] {opCode, authOpCode},
                  new String[] {currentRegionName}, indices, newRnd);
          credentialsTypeStr = " authorized " + authOpCode;
        }

        Properties clientProps =
            concatProperties(new Properties[] {opCredentials, extraAuthProps, extraAuthzProps});
        // Start the client with valid credentials but allowed or disallowed to perform an operation
        System.out.println("executeOpBlock: For client" + clientNum + credentialsTypeStr
            + " credentials: " + opCredentials);
        boolean setupDynamicRegionFactory = (opFlags & OpFlags.ENABLE_DRF) > 0;

        if (useThisVM) {
          SecurityTestUtils.createCacheClientWithDynamicRegion(authInit, clientProps, javaProps, 0,
              setupDynamicRegionFactory, NO_EXCEPTION);
        } else {
          clientVM.invoke("SecurityTestUtils.createCacheClientWithDynamicRegion",
              () -> SecurityTestUtils.createCacheClientWithDynamicRegion(authInit, clientProps,
                  javaProps, 0, setupDynamicRegionFactory, NO_EXCEPTION));
        }
      }

      int expectedResult;
      if ((opFlags & OpFlags.CHECK_NOTAUTHZ) > 0) {
        expectedResult = NOTAUTHZ_EXCEPTION;
      } else if ((opFlags & OpFlags.CHECK_EXCEPTION) > 0) {
        expectedResult = OTHER_EXCEPTION;
      } else {
        expectedResult = NO_EXCEPTION;
      }

      // Perform the operation from selected client
      if (useThisVM) {
        doOp(opCode, currentOp.getIndices(), new Integer(opFlags), new Integer(expectedResult));
      } else {
        int[] indices = currentOp.getIndices();
        clientVM.invoke("ClientAuthorizationTestCase.doOp", () -> ClientAuthorizationTestCase
            .doOp(opCode, indices, new Integer(opFlags), new Integer(expectedResult)));
      }
    }
  }

  protected AuthzCredentialGenerator getXmlAuthzGenerator() {
    AuthzCredentialGenerator authzGen = new XmlAuthzCredentialGenerator();
    CredentialGenerator cGen = new DummyCredentialGenerator();
    cGen.init();
    authzGen.init(cGen);
    return authzGen;
  }

  protected List<AuthzCredentialGenerator> getDummyGeneratorCombos() {
    List<AuthzCredentialGenerator> generators = new ArrayList<>();
    Iterator authzCodeIter = AuthzCredentialGenerator.ClassCode.getAll().iterator();

    while (authzCodeIter.hasNext()) {
      ClassCode authzClassCode = (ClassCode) authzCodeIter.next();
      AuthzCredentialGenerator authzGen = AuthzCredentialGenerator.create(authzClassCode);

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

  protected void runOpsWithFailOver(final OperationWithAction[] opCodes, final String testName)
      throws InterruptedException {
    AuthzCredentialGenerator gen = getXmlAuthzGenerator();
    CredentialGenerator cGen = gen.getCredentialGenerator();
    Properties extraAuthProps = cGen.getSystemProperties();
    Properties javaProps = cGen.getJavaProperties();
    Properties extraAuthzProps = gen.getSystemProperties();
    String authenticator = cGen.getAuthenticator();
    String authInit = cGen.getAuthInit();
    String accessor = gen.getAuthorizationCallback();
    TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(gen);

    System.out.println(testName + ": Using authinit: " + authInit);
    System.out.println(testName + ": Using authenticator: " + authenticator);
    System.out.println(testName + ": Using accessor: " + accessor);

    // Start servers with all required properties
    Properties serverProps =
        buildProperties(authenticator, accessor, false, extraAuthProps, extraAuthzProps);

    // Get ports for the servers
    List<Keeper> randomAvailableTCPPortKeepers =
        AvailablePortHelper.getRandomAvailableTCPPortKeepers(4);
    Keeper locator1PortKeeper = randomAvailableTCPPortKeepers.get(0);
    Keeper locator2PortKeeper = randomAvailableTCPPortKeepers.get(1);
    Keeper port1Keeper = randomAvailableTCPPortKeepers.get(2);
    Keeper port2Keeper = randomAvailableTCPPortKeepers.get(3);
    int locator1Port = locator1PortKeeper.getPort();
    int locator2Port = locator2PortKeeper.getPort();
    int port1 = port1Keeper.getPort();
    int port2 = port2Keeper.getPort();

    // Perform all the ops on the clients
    List opBlock = new ArrayList();
    Random rnd = new Random();

    for (int opNum = 0; opNum < opCodes.length; ++opNum) {
      // Start client with valid credentials as specified in OperationWithAction
      OperationWithAction currentOp = opCodes[opNum];

      if (currentOp.equals(OperationWithAction.OPBLOCK_END)
          || currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
        // End of current operation block; execute all the operations on the servers with/without
        // failover
        if (opBlock.size() > 0) {
          locator1PortKeeper.release();
          port1Keeper.release();

          // Start the first server and execute the operation block
          server1.invoke("createCacheServer", () -> ClientAuthorizationTestCase
              .createCacheServer(locator1Port, port1, serverProps, javaProps));
          server2.invoke("closeCache", () -> closeCache());

          executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
              rnd);

          if (!currentOp.equals(OperationWithAction.OPBLOCK_NO_FAILOVER)) {
            // Failover to the second server and run the block again
            locator2PortKeeper.release();
            port2Keeper.release();

            server2.invoke("createCacheServer", () -> ClientAuthorizationTestCase
                .createCacheServer(locator2Port, port2, serverProps, javaProps));
            server1.invoke("closeCache", () -> closeCache());

            executeOpBlock(opBlock, port1, port2, authInit, extraAuthProps, extraAuthzProps, tgen,
                rnd);
          }
          opBlock.clear();
        }

      } else {
        currentOp.setOpNum(opNum);
        opBlock.add(currentOp);
      }
    }
  }

  /**
   * Implements the {@link CqListener} interface and counts the number of different operations and
   * also queues up the received updates to precise checking of each update.
   *
   * @since GemFire 5.5
   */
  private static class AuthzCqListener implements CqListener {

    private List<CqEvent> eventList;
    private int numCreates;
    private int numUpdates;
    private int numDestroys;
    private int numOtherOps;
    private int numErrors;

    public AuthzCqListener() {
      this.eventList = new ArrayList<>();
      reset();
    }

    public void reset() {
      this.eventList.clear();
      this.numCreates = 0;
      this.numUpdates = 0;
      this.numErrors = 0;
    }

    public void onEvent(final CqEvent aCqEvent) {
      Operation op = aCqEvent.getBaseOperation();
      if (op.isCreate()) {
        ++this.numCreates;
      } else if (op.isUpdate()) {
        ++this.numUpdates;
      } else if (op.isDestroy()) {
        ++this.numDestroys;
      } else {
        ++this.numOtherOps;
      }
      eventList.add(aCqEvent);
    }

    public void onError(final CqEvent aCqEvent) {
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

    public void checkPuts(final String[] vals, final int[] indices) {
      for (int indexIndex = 0; indexIndex < indices.length; ++indexIndex) {
        int index = indices[indexIndex];
        boolean foundKey = false;

        for (Iterator<CqEvent> eventIter = this.eventList.iterator(); eventIter.hasNext();) {
          CqEvent event = (CqEvent) eventIter.next();
          if (KEYS[index].equals(event.getKey())) {
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
   * This class specifies flags that can be used to alter the behaviour of operations being
   * performed by the <code>doOp</code> function.
   *
   * @since GemFire 5.5
   */
  protected static class OpFlags {

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
     * Register all KEYS. For GET operations indicates using getAll().
     */
    public static final int USE_ALL_KEYS = 0x20;

    /**
     * Register a regular expression.
     */
    public static final int USE_REGEX = 0x40;

    /**
     * Register a list of KEYS.
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
     * Do not re-connect using new credentials rather use the previous connection.
     */
    public static final int USE_OLDCONN = 0x1000;

    /**
     * Do the connection with unauthorized credentials but do not check that the operation throws
     * <code>NotAuthorizedException</code>.
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

    public static String description(int f) {
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
   * This class encapsulates an {@link OperationCode} with associated flags, the client to perform
   * the operation, and the number of operations to perform.
   *
   * @since GemFire 5.5
   */
  protected static class OperationWithAction {

    /**
     * The operation to be performed.
     */
    private OperationCode opCode;

    /**
     * The operation for which authorized or unauthorized credentials have to be generated. This is
     * the same as {@link #opCode} when not specified.
     */
    private OperationCode authzOpCode;

    /**
     * The client number on which the operation has to be performed.
     */
    private int clientNum;

    /**
     * Bitwise or'd {@link OpFlags} integer to change/specify the behaviour of the operations.
     */
    private int flags;

    /**
     * Indices of the KEYS array to be used for operations.
     */
    private int[] indices;

    /**
     * An index for the operation used for logging.
     */
    private int opNum;

    /**
     * Indicates end of an operation block which can be used for testing with failover
     */
    public static final OperationWithAction OPBLOCK_END = new OperationWithAction(null, 4);

    /**
     * Indicates end of an operation block which should not be used for testing with failover
     */
    public static final OperationWithAction OPBLOCK_NO_FAILOVER = new OperationWithAction(null, 5);

    private void setIndices(int numOps) {
      this.indices = new int[numOps];
      for (int index = 0; index < numOps; ++index) {
        this.indices[index] = index;
      }
    }

    public OperationWithAction(final OperationCode opCode) {
      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = 1;
      this.flags = OpFlags.NONE;
      setIndices(4);
      this.opNum = 0;
    }

    public OperationWithAction(final OperationCode opCode, final int clientNum) {
      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = clientNum;
      this.flags = OpFlags.NONE;
      setIndices(4);
      this.opNum = 0;
    }

    public OperationWithAction(final OperationCode opCode, final int clientNum, final int flags,
        final int numOps) {
      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = clientNum;
      this.flags = flags;
      setIndices(numOps);
      this.opNum = 0;
    }

    public OperationWithAction(final OperationCode opCode, final OperationCode deniedOpCode,
        final int clientNum, final int flags, final int numOps) {
      this.opCode = opCode;
      this.authzOpCode = deniedOpCode;
      this.clientNum = clientNum;
      this.flags = flags;
      setIndices(numOps);
      this.opNum = 0;
    }

    public OperationWithAction(final OperationCode opCode, final int clientNum, final int flags,
        final int[] indices) {
      this.opCode = opCode;
      this.authzOpCode = opCode;
      this.clientNum = clientNum;
      this.flags = flags;
      this.indices = indices;
      this.opNum = 0;
    }

    public OperationWithAction(final OperationCode opCode, final OperationCode deniedOpCode,
        final int clientNum, final int flags, final int[] indices) {
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

    @Override
    public String toString() {
      return "opCode:" + this.opCode + ",authOpCode:" + this.authzOpCode + ",clientNum:"
          + this.clientNum + ",flags:" + this.flags + ",numOps:" + this.indices.length + ",indices:"
          + indicesToString(this.indices);
    }
  }

  /**
   * Simple interface to generate credentials with authorization based on key indices also. This is
   * utilized by the post-operation authorization tests where authorization is based on key indices.
   *
   * @since GemFire 5.5
   */
  protected interface TestCredentialGenerator {

    /**
     * Get allowed credentials for the given set of operations in the given regions and indices of
     * KEYS in the <code>KEYS</code> array
     */
    public Properties getAllowedCredentials(OperationCode[] opCodes, String[] regionNames,
        int[] keyIndices, int num);

    /**
     * Get disallowed credentials for the given set of operations in the given regions and indices
     * of KEYS in the <code>KEYS</code> array
     */
    public Properties getDisallowedCredentials(OperationCode[] opCodes, String[] regionNames,
        int[] keyIndices, int num);

    /**
     * Get the {@link CredentialGenerator} if any.
     */
    public CredentialGenerator getCredentialGenerator();
  }

  /**
   * Contains a {@link AuthzCredentialGenerator} and implements the {@link TestCredentialGenerator}
   * interface.
   *
   * @since GemFire 5.5
   */
  protected static class TestAuthzCredentialGenerator implements TestCredentialGenerator {

    private AuthzCredentialGenerator authzGen;

    public TestAuthzCredentialGenerator(final AuthzCredentialGenerator authzGen) {
      this.authzGen = authzGen;
    }

    public Properties getAllowedCredentials(final OperationCode[] opCodes,
        final String[] regionNames, final int[] keyIndices, final int num) {
      return this.authzGen.getAllowedCredentials(opCodes, regionNames, num);
    }

    public Properties getDisallowedCredentials(final OperationCode[] opCodes,
        final String[] regionNames, final int[] keyIndices, final int num) {
      return this.authzGen.getDisallowedCredentials(opCodes, regionNames, num);
    }

    public CredentialGenerator getCredentialGenerator() {
      return authzGen.getCredentialGenerator();
    }
  }
}
