//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  public abstract class ThinClientSecurityAuthzTestBase : ThinClientRegionSteps
  {
    #region Protected members

    protected const string SubregionName = "AuthSubregion";
    protected const string CacheXml1 = "cacheserver_notify_subscription.xml";
    protected const string CacheXml2 = "cacheserver_notify_subscription2.xml";

    #endregion

    #region Private methods

    private static string IndicesToString(int[] indices)
    {
      string str = string.Empty;
      if (indices != null && indices.Length > 0)
      {
        str += indices[0];
        for (int index = 1; index < indices.Length; ++index)
        {
          str += ',';
          str += indices[index];
        }
      }
      return str;
    }

    private Region CreateSubregion(Region region)
    {
      Util.Log("CreateSubregion " + SubregionName);
      Region subregion = region.GetSubRegion(SubregionName);
      if (subregion == null)
      {
        //subregion = region.CreateSubRegion(SubregionName, region.Attributes);
        subregion = CacheHelper.GetRegion(region.FullPath).CreateSubRegion(SubregionName, region.Attributes);
      }
      return subregion;
    }

    private bool CheckFlags(OpFlags flags, OpFlags checkFlag)
    {
      return ((flags & checkFlag) == checkFlag);
    }

    protected void DoOp(OperationCode op, int[] indices,
      OpFlags flags, ExpectedResult expectedResult)
    {
      DoOp(op, indices, flags, expectedResult, null, false);
    }

    protected void DoOp(OperationCode op, int[] indices,
      OpFlags flags, ExpectedResult expectedResult, Properties creds, bool isMultiuser)
    {
      Region region;
      if(isMultiuser)
        region = CacheHelper.GetRegion(RegionName, creds);
      else
        region = CacheHelper.GetRegion(RegionName);

      if (CheckFlags(flags, OpFlags.UseSubRegion))
      {
        Region subregion = null;
        if (CheckFlags(flags, OpFlags.NoCreateSubRegion))
        {
          subregion = region.GetSubRegion(SubregionName);
          if (CheckFlags(flags, OpFlags.CheckNoRegion))
          {
            Assert.IsNull(subregion);
            return;
          }
          else
          {
            Assert.IsNotNull(subregion);
          }
        }
        else
        {
          subregion = CreateSubregion(region);
          if (isMultiuser)
            subregion = region.GetSubRegion(SubregionName);
        }
        Assert.IsNotNull(subregion);
        region = subregion;
      }
      else if (CheckFlags(flags, OpFlags.CheckNoRegion))
      {
        Assert.IsNull(region);
        return;
      }
      else
      {
        Assert.IsNotNull(region);
      }
      string valPrefix;
      if (CheckFlags(flags, OpFlags.UseNewVal))
      {
        valPrefix = NValuePrefix;
      }
      else
      {
        valPrefix = ValuePrefix;
      }
      int numOps = indices.Length;
      Util.Log("Got DoOp for op: " + op + ", numOps: " + numOps
              + ", indices: " + IndicesToString(indices));
      bool exceptionOccured = false;
      bool breakLoop = false;
      for (int indexIndex = 0; indexIndex < indices.Length; ++indexIndex)
      {
        if (breakLoop)
        {
          break;
        }
        int index = indices[indexIndex];
        string key = KeyPrefix + index;
        CacheableString expectedValue = new CacheableString(valPrefix + index);
        try
        {
          switch (op)
          {
            case OperationCode.Get:
              IGFSerializable value = null;
              if (CheckFlags(flags, OpFlags.LocalOp))
              {
                int sleepMillis = 100;
                int numTries = 30;
                bool success = false;
                while (!success && numTries-- > 0)
                {
                  if (!isMultiuser && region.ContainsValueForKey(key))
                  {
                    value = region.Get(key);
                    success = expectedValue.Equals(value);
                    if (CheckFlags(flags, OpFlags.CheckFail))
                    {
                      success = !success;
                    }
                  }
                  else
                  {
                    value = null;
                    success = CheckFlags(flags, OpFlags.CheckFail);
                  }
                  if (!success)
                  {
                    Thread.Sleep(sleepMillis);
                  }
                }
              }
              else
              {
                if (!isMultiuser)
                {
                  if (CheckFlags(flags, OpFlags.CheckNoKey))
                  {
                    Assert.IsFalse(region.ContainsKey(key));
                  }
                  else
                  {
                    Assert.IsTrue(region.ContainsKey(key));
                    region.LocalInvalidate(key);
                  }
                }
                value = region.Get(key);
              }

              //TODO:hitesh ??
              if (!isMultiuser)
              {
                if (CheckFlags(flags, OpFlags.CheckFail))
                {
                  Assert.AreNotEqual(expectedValue, value);
                }
                else
                {
                  Assert.AreEqual(expectedValue, value);
                }
              }
              break;

            case OperationCode.Put:
              region.Put(key, expectedValue);
              break;

            case OperationCode.Destroy:
              if (!isMultiuser && !region.ContainsKey(key))
              {
                // Since DESTROY will fail unless the value is present
                // in the local cache, this is a workaround for two cases:
                // 1. When the operation is supposed to succeed then in
                // the current AuthzCredentialGenerators the clients having
                // DESTROY permission also has CREATE/UPDATE permission
                // so that calling region.Put() will work for that case.
                // 2. When the operation is supposed to fail with
                // NotAuthorizedException then in the current
                // AuthzCredentialGenerators the clients not
                // having DESTROY permission are those with reader role that have
                // GET permission.
                //
                // If either of these assumptions fails, then this has to be
                // adjusted or reworked accordingly.
                if (CheckFlags(flags, OpFlags.CheckNotAuthz))
                {
                  value = region.Get(key);
                  Assert.AreEqual(expectedValue, value);
                }
                else
                {
                  region.Put(key, expectedValue);
                }
              }
              if ( !isMultiuser && CheckFlags(flags, OpFlags.LocalOp))
              {
                region.LocalDestroy(key);
              }
              else
              {
                region.Destroy(key);
              }
              break;

            case OperationCode.RegisterInterest:
              if (CheckFlags(flags, OpFlags.UseList))
              {
                breakLoop = true;
                // Register interest list in this case
                List<CacheableKey> keyList = new List<CacheableKey>(numOps);
                for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex)
                {
                  int keyNum = indices[keyNumIndex];
                  keyList.Add(KeyPrefix + keyNum);
                }
                region.RegisterKeys(keyList.ToArray());
              }
              else if (CheckFlags(flags, OpFlags.UseRegex))
              {
                breakLoop = true;
                region.RegisterRegex(KeyPrefix + "[0-" + (numOps - 1) + ']');
              }
              else if (CheckFlags(flags, OpFlags.UseAllKeys))
              {
                breakLoop = true;
                region.RegisterAllKeys();
              }
              break;

            case OperationCode.UnregisterInterest:
              if (CheckFlags(flags, OpFlags.UseList))
              {
                breakLoop = true;
                // Register interest list in this case
                List<CacheableKey> keyList = new List<CacheableKey>(numOps);
                for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex)
                {
                  int keyNum = indices[keyNumIndex];
                  keyList.Add(KeyPrefix + keyNum);
                }
                region.UnregisterKeys(keyList.ToArray());
              }
              else if (CheckFlags(flags, OpFlags.UseRegex))
              {
                breakLoop = true;
                region.UnregisterRegex(KeyPrefix + "[0-" + (numOps - 1) + ']');
              }
              else if (CheckFlags(flags, OpFlags.UseAllKeys))
              {
                breakLoop = true;
                region.UnregisterAllKeys();
              }
              break;

            case OperationCode.Query:
              breakLoop = true;
              ResultSet queryResults;

              if (!isMultiuser)
              {
                queryResults = (ResultSet)region.Query(
                  "SELECT DISTINCT * FROM " + region.FullPath);
              }
              else
              {
                queryResults = (ResultSet)CacheHelper.getMultiuserCache(creds).GetQueryService().NewQuery("SELECT DISTINCT * FROM " + region.FullPath).Execute();
              }
              Assert.IsNotNull(queryResults);
              if (!CheckFlags(flags, OpFlags.CheckFail))
              {
                Assert.AreEqual(numOps, queryResults.Size);
              }
              CacheableHashSet querySet = new CacheableHashSet(
                queryResults.Size);
              foreach (CacheableString result in queryResults)
              {
                querySet.Add(result);
              }
              for (int keyNumIndex = 0; keyNumIndex < numOps; ++keyNumIndex)
              {
                int keyNum = indices[keyNumIndex];
                CacheableString expectedVal = new CacheableString(
                  valPrefix + keyNumIndex);
                if (CheckFlags(flags, OpFlags.CheckFail))
                {
                  Assert.IsFalse(querySet.Contains(expectedVal));
                }
                else
                {
                  Assert.IsTrue(querySet.Contains(expectedVal));
                }
              }
              break;

            case OperationCode.RegionDestroy:
              breakLoop = true;
              if ( !isMultiuser && CheckFlags(flags, OpFlags.LocalOp))
              {
                region.LocalDestroyRegion();
              }
              else
              {
                region.DestroyRegion();
              }
              break;

            case OperationCode.GetServerKeys:
              breakLoop = true;
              region.GetServerKeys();
              break;

            case OperationCode.GetAll:
              ICacheableKey[] keymap = new ICacheableKey[5];
              for (int i = 0; i < 5; i++)
              {
                CacheableInt32 item = CacheableInt32.Create(i);
                // NOTE: GetAll should operate right after PutAll
                keymap[i] = item;
              }
              CacheableHashMap entrymap = CacheableHashMap.Create();
              region.GetAll(keymap, entrymap, null, false);
              if (entrymap.Count < 5)
              {
                Assert.Fail("DoOp: Got fewer entries for op " + op);
              }
              break;

            case OperationCode.PutAll:
              // NOTE: PutAll should operate right before GetAll
              CacheableHashMap entrymap2 = CacheableHashMap.Create();
              for (int i = 0; i < 5; i++)
              {
                CacheableInt32 item = CacheableInt32.Create(i);
                entrymap2.Add(item, item);
              }
              region.PutAll(entrymap2);
              break;
              
            case OperationCode.ExecuteCQ:
              Pool pool = PoolManager.Find("__TESTPOOL1_");
              QueryService qs;
              if (pool != null)
              {
                qs = pool.GetQueryService();
              }
              else
              {
                qs = CacheHelper.DCache.GetQueryService();
              }
              CqAttributesFactory cqattrsfact = new CqAttributesFactory();
              CqAttributes cqattrs = cqattrsfact.Create();
              CqQuery cq = qs.NewCq("cq_security", "SELECT * FROM /" + region.Name, cqattrs, false);
              qs.ExecuteCqs();
              qs.StopCqs();
              qs.CloseCqs();
              break;

            case OperationCode.ExecuteFunction:
              if (!isMultiuser)
              {
                Pool pool2 = PoolManager.Find("__TESTPOOL1_");
                if (pool2 != null)
                {
                  FunctionService.OnServer(pool2).Execute("securityTest", true);
                  FunctionService.OnRegion(region).Execute("FireNForget");
                }
                else
                {
                  expectedResult = ExpectedResult.Success;
                }
              }
              else
              {
                //FunctionService fs = CacheHelper.getMultiuserCache(creds).GetFunctionService();
                //Execution exe =  fs.OnServer();
                IRegionService userCache = CacheHelper.getMultiuserCache(creds);
                Execution exe = FunctionService.OnServer(userCache);
                exe.Execute("securityTest", true);
                exe = FunctionService.OnServers(userCache);
                FunctionService.OnRegion(region);
                FunctionService.OnRegion(userCache.GetRegion(region.Name)).Execute("FireNForget");
              }

              break;

            default:
              Assert.Fail("DoOp: Unhandled operation " + op);
              break;
          }
          if (expectedResult != ExpectedResult.Success)
          {
            Assert.Fail("Expected an exception while performing operation");
          }
        }
        catch (AssertionException ex)
        {
          Util.Log("DoOp: failed assertion: {0}", ex);
          throw;
        }
        catch (NotAuthorizedException ex)
        {
          exceptionOccured = true;
          if (expectedResult == ExpectedResult.NotAuthorizedException)
          {
            Util.Log(
                "DoOp: Got expected NotAuthorizedException when doing operation ["
                    + op + "] with flags [" + flags + "]: " + ex.Message);
            continue;
          }
          else
          {
            Assert.Fail("DoOp: Got unexpected NotAuthorizedException when " +
              "doing operation: " + ex.Message);
          }
        }
        catch (Exception ex)
        {
          exceptionOccured = true;
          if (expectedResult == ExpectedResult.OtherException)
          {
            Util.Log("DoOp: Got expected exception when doing operation: " +
              ex.GetType() + "::" + ex.Message);
            continue;
          }
          else
          {
            Assert.Fail("DoOp: Got unexpected exception when doing operation: " + ex);
          }
        }
      }
      if (!exceptionOccured
          && expectedResult != ExpectedResult.Success)
      {
        Assert.Fail("Expected an exception while performing operation");
      }
    }

    protected void ExecuteOpBlock(List<OperationWithAction> opBlock,
      string authInit, Properties extraAuthProps, Properties extraAuthzProps,
      TestCredentialGenerator gen, Random rnd, bool pool, bool locator, bool isMultiuser, bool ssl, bool withPassword)
    {
      foreach (OperationWithAction currentOp in opBlock)
      {
        // Start client with valid credentials as specified in
        // OperationWithAction
        OperationCode opCode = currentOp.OpCode;
        OpFlags opFlags = currentOp.Flags;
        int clientNum = currentOp.ClientNum;
        if (clientNum > m_clients.Length)
        {
          Assert.Fail("ExecuteOpBlock: Unknown client number " + clientNum);
        }
        ClientBase client = m_clients[clientNum - 1];
        Util.Log("ExecuteOpBlock: performing operation number [" +
          currentOp.OpNum + "]: " + currentOp);
        Properties clientProps = null;
        if (!CheckFlags(opFlags, OpFlags.UseOldConn))
        {
          Properties opCredentials;
          int newRnd = rnd.Next(100) + 1;
          string currentRegionName = '/' + RegionName;
          if (CheckFlags(opFlags, OpFlags.UseSubRegion))
          {
            currentRegionName += ('/' + SubregionName);
          }
          string credentialsTypeStr;
          OperationCode authOpCode = currentOp.AuthzOperationCode;
          int[] indices = currentOp.Indices;
          CredentialGenerator cGen = gen.GetCredentialGenerator();
          Properties javaProps = null;
          if (CheckFlags(opFlags, OpFlags.CheckNotAuthz) ||
            CheckFlags(opFlags, OpFlags.UseNotAuthz))
          {
            opCredentials = gen.GetDisallowedCredentials(
              new OperationCode[] { authOpCode },
              new string[] { currentRegionName }, indices, newRnd);
            credentialsTypeStr = " unauthorized " + authOpCode;
          }
          else
          {
            opCredentials = gen.GetAllowedCredentials(new OperationCode[] {
              opCode, authOpCode }, new string[] { currentRegionName },
              indices, newRnd);
            credentialsTypeStr = " authorized " + authOpCode;
          }
          if (cGen != null)
          {
            javaProps = cGen.JavaProperties;
          }
          clientProps = SecurityTestUtil.ConcatProperties(
            opCredentials, extraAuthProps, extraAuthzProps);
          // Start the client with valid credentials but allowed or disallowed to
          // perform an operation
          Util.Log("ExecuteOpBlock: For client" + clientNum +
            credentialsTypeStr + " credentials: " + opCredentials);
          
          if(!isMultiuser)
            client.Call(SecurityTestUtil.CreateClientSSL, RegionName,
              CacheHelper.Endpoints, CacheHelper.Locators, authInit, clientProps, pool, locator, ssl, withPassword);
          else
            client.Call(SecurityTestUtil.CreateClientMU, RegionName,
              CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);
        }
        ExpectedResult expectedResult;
        if (CheckFlags(opFlags, OpFlags.CheckNotAuthz))
        {
          expectedResult = ExpectedResult.NotAuthorizedException;
        }
        else if (CheckFlags(opFlags, OpFlags.CheckException))
        {
          expectedResult = ExpectedResult.OtherException;
        }
        else
        {
          expectedResult = ExpectedResult.Success;
        }

        // Perform the operation from selected client
        if (!isMultiuser)
          client.Call(DoOp, opCode, currentOp.Indices, opFlags, expectedResult);
        else
          client.Call(DoOp, opCode, currentOp.Indices, opFlags, expectedResult, clientProps, true);
      }
    }

    protected List<AuthzCredentialGenerator> GetAllGeneratorCombos(bool isMultiUser)
    {
      List<AuthzCredentialGenerator> generators =
        new List<AuthzCredentialGenerator>();
      foreach (AuthzCredentialGenerator.ClassCode authzClassCode in
        Enum.GetValues(typeof(AuthzCredentialGenerator.ClassCode)))
      {
        List<CredentialGenerator> cGenerators =
          SecurityTestUtil.getAllGenerators(isMultiUser);
        foreach (CredentialGenerator cGen in cGenerators)
        {
          AuthzCredentialGenerator authzGen = AuthzCredentialGenerator
              .Create(authzClassCode);
          if (authzGen != null)
          {
            if (authzGen.Init(cGen))
            {
              generators.Add(authzGen);
            }
          }
        }
      }
      return generators;
    }

    protected void RunOpsWithFailoverSSL(OperationWithAction[] opCodes,
        string testName, bool pool, bool locator, bool withPassword)
    {
      RunOpsWithFailover(opCodes, testName, pool, locator, false, true, withPassword);
    }

    protected void RunOpsWithFailover(OperationWithAction[] opCodes,
        string testName, bool pool, bool locator)
    {
      RunOpsWithFailover(opCodes, testName, pool, locator, false);
    }

    protected void RunOpsWithFailover(OperationWithAction[] opCodes,
        string testName, bool pool, bool locator, bool isMultiUser)
    {
      RunOpsWithFailover(opCodes, testName, pool, locator, isMultiUser, false, false);
    }

    protected void RunOpsWithFailover(OperationWithAction[] opCodes,
        string testName, bool pool, bool locator, bool isMultiUser, bool ssl, bool withPassword)
    {
      CacheHelper.SetupJavaServers(pool && locator, CacheXml1, CacheXml2);
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC", null, ssl);
        Util.Log("Locator started");
      }
      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(isMultiUser))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();        
        Properties extraAuthProps = cGen.SystemProperties;
        Properties javaProps = cGen.JavaProperties;
        Properties extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;
        TestAuthzCredentialGenerator tgen = new TestAuthzCredentialGenerator(authzGen);

        Util.Log(testName + ": Using authinit: " + authInit);
        Util.Log(testName + ": Using authenticator: " + authenticator);
        Util.Log(testName + ": Using accessor: " + accessor);

        // Start servers with all required properties
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);

        // Perform all the ops on the clients
        List<OperationWithAction> opBlock = new List<OperationWithAction>();
        Random rnd = new Random();
        for (int opNum = 0; opNum < opCodes.Length; ++opNum)
        {
          // Start client with valid credentials as specified in
          // OperationWithAction
          OperationWithAction currentOp = opCodes[opNum];
          if (currentOp == OperationWithAction.OpBlockEnd ||
            currentOp == OperationWithAction.OpBlockNoFailover)
          {
            // End of current operation block; execute all the operations
            // on the servers with/without failover
            if (opBlock.Count > 0)
            {
              // Start the first server and execute the operation block
              if (pool && locator)
              {
                CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs, ssl);
              }
              else
              {
                CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
              }
              Util.Log("Cacheserver 1 started.");
              CacheHelper.StopJavaServer(2, false);
              ExecuteOpBlock(opBlock, authInit, extraAuthProps,
                extraAuthzProps, tgen, rnd, pool, locator, isMultiUser, ssl, withPassword);
              if (currentOp == OperationWithAction.OpBlockNoFailover)
              {
                CacheHelper.StopJavaServer(1);
              }
              else
              {
                // Failover to the second server and run the block again
                if (pool && locator)
                {
                  CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs, ssl);
                }
                else
                {
                  CacheHelper.StartJavaServer(2, "GFECS2", serverArgs);
                }
                Util.Log("Cacheserver 2 started.");
                CacheHelper.StopJavaServer(1);
                ExecuteOpBlock(opBlock, authInit, extraAuthProps,
                  extraAuthzProps, tgen, rnd, pool, locator, isMultiUser, ssl,withPassword);
              }
              opBlock.Clear();
            }
          }
          else
          {
            currentOp.OpNum = opNum;
            opBlock.Add(currentOp);
          }
        }
        // Close all clients here since we run multiple iterations for pool and non pool configs
        foreach (ClientBase client in m_clients)
        {
          client.Call(Close);
        }
      }
      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1, true, ssl);
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    #endregion

    /// <summary>
    /// This class specifies flags that can be used to alter the behaviour of
    /// operations being performed by the <see cref="DoOp"/> method.
    /// </summary>
    [Flags]
    public enum OpFlags
    {
      /// <summary>
      /// Default behaviour.
      /// </summary>
      None = 0x0,

      /// <summary>
      /// Check that the operation should fail.
      /// </summary>
      CheckFail = 0x1,

      /// <summary>
      /// Check that the operation should throw <c>NotAuthorizedException</c>.
      /// </summary>
      CheckNotAuthz = 0x2,

      /// <summary>
      /// Do the connection with unauthorized credentials but do not check that the
      /// operation throws <c>NotAuthorizedException</c>.
      /// </summary>
      UseNotAuthz = 0x4,

      /// <summary>
      /// Check that the region should not be available.
      /// </summary>
      CheckNoRegion = 0x8,

      /// <summary>
      /// Check that the operation should throw an exception other than the
      /// <c>NotAuthorizedException</c>.
      /// </summary>
      CheckException = 0x10,

      /// <summary>
      /// Check for values starting with <c>NValuePrefix</c> instead of
      /// <c>ValuePrefix</c>.
      /// </summary>
      UseNewVal = 0x20,

      /// <summary>
      /// Register a regular expression.
      /// </summary>
      UseRegex = 0x40,

      /// <summary>
      /// Register a list of keys.
      /// </summary>
      UseList = 0x80,

      /// <summary>
      /// Register all keys.
      /// </summary>
      UseAllKeys = 0x100,

      /// <summary>
      /// Perform the local version of the operation (if applicable).
      /// </summary>
      LocalOp = 0x200,

      /// <summary>
      /// Check that the key for the operation should not be present.
      /// </summary>
      CheckNoKey = 0x400,

      /// <summary>
      /// Use the sub-region for performing the operation.
      /// </summary>
      UseSubRegion = 0x800,

      /// <summary>
      /// Do not try to create the sub-region.
      /// </summary>
      NoCreateSubRegion = 0x1000,

      /// <summary>
      /// Do not re-connect using new credentials rather use the previous
      /// connection.
      /// </summary>
      UseOldConn = 0x2000,
    }

    /// <summary>
    /// This class encapsulates an <see cref="OperationCode"/> with associated flags, the
    /// client to perform the operation, and the number of operations to perform.
    /// </summary>
    public class OperationWithAction
    {
      /// <summary>
      /// The operation to be performed.
      /// </summary>
      private OperationCode m_opCode;

      /// <summary>
      /// The operation for which authorized or unauthorized credentials have to be
      /// generated. This is the same as {@link #opCode} when not specified.
      /// </summary>
      private OperationCode m_authzOpCode;

      /// <summary>
      /// The client number on which the operation has to be performed.
      /// </summary>
      private int m_clientNum;

      /// <summary>
      /// Bitwise or'd <see cref="OpFlags"/> to change/specify the behaviour of
      /// the operations.
      /// </summary>
      private OpFlags m_flags;

      /// <summary>
      /// Indices of the keys array to be used for operations. The keys used
      /// will be concatenation of <c>KeyPrefix</c> and <c>index</c> integer.
      /// </summary>
      private int[] m_indices;

      /// <summary>
      /// An index for the operation used for logging.
      /// </summary>
      private int m_opNum;

      /// <summary>
      /// Indicates end of an operation block which can be used for testing with
      /// failover.
      /// </summary>
      public static readonly OperationWithAction OpBlockEnd = new OperationWithAction(
          OperationCode.Get, 4);

      /// <summary>
      /// Indicates end of an operation block which should not be used for testing
      /// with failover.
      /// </summary>
      public static readonly OperationWithAction OpBlockNoFailover =
        new OperationWithAction(OperationCode.Get, 5);

      private void SetIndices(int numOps)
      {
        this.m_indices = new int[numOps];
        for (int index = 0; index < numOps; ++index)
        {
          this.m_indices[index] = index;
        }
      }

      public OperationWithAction(OperationCode opCode)
      {
        this.m_opCode = opCode;
        this.m_authzOpCode = opCode;
        this.m_clientNum = 1;
        this.m_flags = OpFlags.None;
        SetIndices(4);
        this.m_opNum = 0;
      }

      public OperationWithAction(OperationCode opCode, int clientNum)
      {
        this.m_opCode = opCode;
        this.m_authzOpCode = opCode;
        this.m_clientNum = clientNum;
        this.m_flags = OpFlags.None;
        SetIndices(4);
        this.m_opNum = 0;
      }

      public OperationWithAction(OperationCode opCode, int clientNum, OpFlags flags,
          int numOps)
      {
        this.m_opCode = opCode;
        this.m_authzOpCode = opCode;
        this.m_clientNum = clientNum;
        this.m_flags = flags;
        SetIndices(numOps);
        this.m_opNum = 0;
      }

      public OperationWithAction(OperationCode opCode,
          OperationCode deniedOpCode, int clientNum, OpFlags flags, int numOps)
      {
        this.m_opCode = opCode;
        this.m_authzOpCode = deniedOpCode;
        this.m_clientNum = clientNum;
        this.m_flags = flags;
        SetIndices(numOps);
        this.m_opNum = 0;
      }

      public OperationWithAction(OperationCode opCode, int clientNum,
        OpFlags flags, int[] indices)
      {
        this.m_opCode = opCode;
        this.m_authzOpCode = opCode;
        this.m_clientNum = clientNum;
        this.m_flags = flags;
        this.m_indices = indices;
        this.m_opNum = 0;
      }

      public OperationWithAction(OperationCode opCode, OperationCode authzOpCode,
        int clientNum, OpFlags flags, int[] indices)
      {
        this.m_opCode = opCode;
        this.m_authzOpCode = authzOpCode;
        this.m_clientNum = clientNum;
        this.m_flags = flags;
        this.m_indices = indices;
        this.m_opNum = 0;
      }

      public OperationCode OpCode
      {
        get
        {
          return this.m_opCode;
        }
      }

      public OperationCode AuthzOperationCode
      {
        get
        {
          return this.m_authzOpCode;
        }
      }

      public int ClientNum
      {
        get
        {
          return this.m_clientNum;
        }
      }

      public OpFlags Flags
      {
        get
        {
          return this.m_flags;
        }
      }

      public int[] Indices
      {
        get
        {
          return this.m_indices;
        }
      }

      public int OpNum
      {
        get
        {
          return this.m_opNum;
        }
        set
        {
          this.m_opNum = value;
        }
      }

      public override string ToString()
      {
        return "opCode:" + this.m_opCode + ",authOpCode:" + this.m_authzOpCode
            + ",clientNum:" + this.m_clientNum + ",flags:" + this.m_flags
            + ",numOps:" + this.m_indices.Length + ",indices:"
            + IndicesToString(this.m_indices);
      }
    }

    /// <summary>
    /// Simple interface to generate credentials with authorization based on key
    /// indices also. This is utilized by the post-operation authorization tests
    /// <c>ThinClientAuthzObjectModTests</c> where authorization depends on
    /// the actual keys being used for the operation.
    /// </summary>
    public interface TestCredentialGenerator
    {
      /// <summary>
      /// Get allowed credentials for the given set of operations in the given
      /// regions and indices of keys.
      /// </summary>
      Properties GetAllowedCredentials(OperationCode[] opCodes,
          string[] regionNames, int[] keyIndices, int num);

      /// <summary>
      /// Get disallowed credentials for the given set of operations in the given
      /// regions and indices of keys.
      /// </summary>
      Properties GetDisallowedCredentials(OperationCode[] opCodes,
          string[] regionNames, int[] keyIndices, int num);

      /// <summary>
      /// Get the <see cref="CredentialGenerator"/> if any.
      /// </summary>
      /// <returns></returns>
      CredentialGenerator GetCredentialGenerator();
    }

    /// <summary>
    /// Contains a <c>AuthzCredentialGenerator</c> and implements the
    /// <c>TestCredentialGenerator</c> interface.
    /// </summary>
    protected class TestAuthzCredentialGenerator : TestCredentialGenerator
    {
      private AuthzCredentialGenerator authzGen;

      public TestAuthzCredentialGenerator(AuthzCredentialGenerator authzGen)
      {
        this.authzGen = authzGen;
      }

      public Properties GetAllowedCredentials(OperationCode[] opCodes,
          string[] regionNames, int[] keyIndices, int num)
      {

        return this.authzGen.GetAllowedCredentials(opCodes, regionNames, num);
      }

      public Properties GetDisallowedCredentials(OperationCode[] opCodes,
          string[] regionNames, int[] keyIndices, int num)
      {

        return this.authzGen.GetDisallowedCredentials(opCodes, regionNames, num);
      }

      public CredentialGenerator GetCredentialGenerator()
      {

        return authzGen.GetCredentialGenerator();
      }
    }
  }
}
