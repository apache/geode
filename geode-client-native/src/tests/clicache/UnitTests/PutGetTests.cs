//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class PutGetTests : UnitTests
  {
    #region Private members and constants

    public const int NumKeys = 20;
    public const int KeySize = 256;
    public const int ValueSize = 4096;
    private const string RegionName = "PutGetTest";
    private const string KeyChecksumPrefix = "KeyChecksum:";
    private const string ValChecksumPrefix = "ValChecksum:";
    private UnitProcess m_client1, m_client2;
    private Region m_region;
    private CacheableKeyWrapper[] m_cKeys;
    private uint[] m_cKeyCksums;
    private CacheableWrapper[] m_cValues;
    private uint[] m_cValCksums;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    #region Public accessors

    public CacheableWrapper[] CacheableKeys
    {
      get
      {
        return m_cKeys;
      }
    }

    public CacheableWrapper[] CacheableValues
    {
      get
      {
        return m_cValues;
      }
    }

    #endregion

    #region Private functions

    private Type GetValueType()
    {
      Type valType = null;
      if (m_cValues[0].Cacheable != null) {
        valType = m_cValues[0].Cacheable.GetType();
      }
      return valType;
    }

    #endregion

    #region Functions invoked by the tests

    /// <summary>
    /// Initialize the keys for different key types.
    /// </summary>
    public int InitKeys(UInt32 typeId, int numKeys, int maxSize)
    {
      Util.Log("InitKeys typeId " + typeId + " numKeys= " + numKeys + "maxSize=" + maxSize);
      Assert.Greater(numKeys, 0,
        "Number of keys should be greater than zero.");
      Type type = CacheableWrapperFactory.GetTypeForId(typeId);
      CacheableKeyWrapper instance =
        CacheableWrapperFactory.CreateKeyInstance(typeId);
      Assert.IsNotNull(instance, "InitKeys: Type '{0}' could not be instantiated.",
        type.Name);
      int maxKeys = instance.MaxKeys;
      if (numKeys > maxKeys) {
        numKeys = maxKeys;
      }
      m_cKeys = new CacheableKeyWrapper[numKeys];
      m_cKeyCksums = new uint[numKeys];

      for (int keyIndex = 0; keyIndex < numKeys; keyIndex++) {
        instance = CacheableWrapperFactory.CreateKeyInstance(typeId);
        instance.InitKey(keyIndex, maxSize);
        m_cKeyCksums[keyIndex] = instance.GetChecksum();
        m_cKeys[keyIndex] = instance;
      }

      Util.Log("InitKeys final m_cKeyCksums " + m_cKeyCksums.Length + " m_cKeys:" + m_cKeys.Length + "numKeys: " + numKeys);

      return numKeys;
    }

    /// <summary>
    /// Initialize the values to random values for different value types.
    /// </summary>
    public void InitValues(UInt32 typeId, int numValues, int maxSize)
    {
      Util.Log("InitValues typeId " + typeId + " numKeys= " + numValues + "maxSize=" + maxSize);
      Assert.Greater(numValues, 0,
        "Number of values should be greater than zero.");
      Type type = CacheableWrapperFactory.GetTypeForId(typeId);
      m_cValues = new CacheableWrapper[numValues];
      m_cValCksums = new uint[numValues];
      CacheableWrapper instance;
      for (int valIndex = 0; valIndex < numValues; valIndex++) {
        instance = CacheableWrapperFactory.CreateInstance(typeId);
        Util.Log("hitesh in initvalue type " + instance.GetType().ToString());
        Assert.IsNotNull(instance, "InitValues: Type '{0}' could not be instantiated.",
          type.Name);
        instance.InitRandomValue(maxSize);
        m_cValCksums[valIndex] = instance.GetChecksum();
        m_cValues[valIndex] = instance;
      }

      Util.Log("InitValues final m_cValCksums " + m_cValCksums.Length + " m_cValues:" + m_cValues.Length); 
    }

    public void SetRegion(string regionName)
    {
      m_region = CacheHelper.GetVerifyRegionAD(regionName);
    }

    public void DoPuts()
    {
      Assert.IsNotNull(m_cKeys, "DoPuts: null keys array.");
      Assert.IsNotNull(m_cValues, "DoPuts: null values array.");
      Assert.IsNotNull(m_region, "DoPuts: null region.");

      for (int keyIndex = 0; keyIndex < m_cKeys.Length; keyIndex++) {
        ICacheableKey key = m_cKeys[keyIndex].CacheableKey;
        IGFSerializable val = m_cValues[keyIndex].Cacheable;
        if (val != null) {
          Util.Log("hitesh DoPuts() key hashcode " + key.GetHashCode() );
          Util.Log("hitesh DoPuts() " + key.GetType().ToString() + " : " + val.GetType().ToString());
          m_region.Put(key, val);
        }
        else {
          try {
            m_region.Destroy(key);
          }
          catch (EntryNotFoundException) {
            // expected
          }
          m_region.Create(key, val);
        }
      }
      Util.Log("DoPuts completed for keyType [{0}], valType [{1}].",
        m_cKeys[0].CacheableKey.GetType(), GetValueType());
    }

    public void DoHashPuts()
    {
      Assert.IsNotNull(m_cKeys, "DoPuts: null keys array.");
      Assert.IsNotNull(m_region, "DoPuts: null region.");

      for (int keyIndex = 0; keyIndex < m_cKeys.Length; keyIndex++) {
        ICacheableKey key = m_cKeys[keyIndex].CacheableKey;
        int val = key.GetHashCode();
        m_region.Put(key, val);
      }
    }

    public void DoPRSHPartitionResolverPuts(string rname)
    {
      Region region = CacheHelper.GetRegion(rname);
      int nonSingleHopCount = 0, metadatarefreshCount = 0;
      int nonSingleHopCount1 = 0, metadatarefreshCount1 = 0;
      Assert.IsNotNull(region, "DoPRSHPartitionResolverPuts: null region.");
      if (rname.CompareTo("DistRegionNoAck") == 0) {
        Util.Log("Inside DoPRSHPartitionResolverPuts region name is {0} ", region.Name.ToString());             
        for (int i = 0; i < 1000; i++) {
          try {
            CacheableInt32 key = new CacheableInt32(i);
            int val = key.GetHashCode();
            region.Put(key, val);
            Util.Log("Put inside DoPRSHPartitionResolverPuts successfull {0} {1}", key, val);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }
        }        
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("RegionStatistics");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            nonSingleHopCount = rStats.GetInt((string)"nonSingleHopCount");
            metadatarefreshCount = rStats.GetInt((string)"metaDataRefreshCount");            
          }
        }
        Assert.AreEqual(nonSingleHopCount, 0, "nonSingleHopCount should be equal to 0");
        Assert.Greater(metadatarefreshCount, 0, "metadatarefreshCount1 should be greater than 0");

        Util.Log("DoPRSHPartitionResolverPuts WarmUp Task completed.");
        
        for (int i = 1000; i < 2000; i++) {
          try {
            CacheableInt32 key = new CacheableInt32(i);
            int val = key.GetHashCode();
            region.Put(key, val);
            Util.Log("Put inside DoPRSHPartitionResolverPuts successfull {0} {1}", key, val);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }

          try {
            CacheableInt32 key = new CacheableInt32(i);
            region.Get(key);
            Util.Log("Get inside DoPRSHPartitionResolverPuts successfull {0}", key);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Get caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Get caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Get caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }

          try {
            CacheableInt32 key = new CacheableInt32(i);
            region.Destroy(key);
            Util.Log("Destroy inside DoPRSHPartitionResolverPuts successfull {0}", key);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Destroy caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Destroy caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Destroy caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }
        }       
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            nonSingleHopCount1 = rStats.GetInt((string)"nonSingleHopCount");
            metadatarefreshCount1 = rStats.GetInt((string)"metaDataRefreshCount");            
            Assert.AreEqual(nonSingleHopCount1, nonSingleHopCount, "nonSingleHopCount1 should be equal to nonSingleHopCount");
            Assert.AreEqual(metadatarefreshCount1, metadatarefreshCount, "metadatarefreshCount1 should be equal to metadatarefreshCount");
          }
        }
      }
      nonSingleHopCount = metadatarefreshCount = 0;
      if (rname.CompareTo("DistRegionAck") == 0) {
        Util.Log("Inside DoPRSHPartitionResolverPuts region name is {0} ", region.Name.ToString());
        for (int i = 1000; i < 2000; i++) {
          try {
            CacheableInt32 key = new CacheableInt32(i);
            int val = key.GetHashCode();
            region.Put(key, val);
            Util.Log("Put inside DoPRSHPartitionResolverPuts successfull {0} {1}", key, val);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }
          try {
            CacheableInt32 key = new CacheableInt32(i);
            region.Get(key);
            Util.Log("Get inside DoPRSHPartitionResolverPuts successfull {0}", key);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Get caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Get caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Get caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }
          try {
            CacheableInt32 key = new CacheableInt32(i);
            region.Destroy(key);
            Util.Log("Destroy inside DoPRSHPartitionResolverPuts successfull {0}", key);
          }
          catch (CacheServerException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheServerException (0}", ex.Message);
          }
          catch (CacheWriterException ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got CacheWriterException (0}", ex.Message);
          }
          catch (Exception ex) {
            Util.Log("CPPTEST: Put caused networkhop");
            Assert.Fail("Got Exception (0}", ex.Message);
          }
        }
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("RegionStatistics");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            nonSingleHopCount = rStats.GetInt((string)"nonSingleHopCount");
            metadatarefreshCount = rStats.GetInt((string)"metaDataRefreshCount");           
          }
        }
        Assert.AreEqual(nonSingleHopCount, 0, "nonSingleHopCount should be equal to 0");
        Assert.Greater(metadatarefreshCount, 0, "metadatarefreshCount2 should be greater than 0");
      }
    }

    public void DoPRSHFixedPartitionResolverTests(string rname)
    {
      int nonSingleHopCount = 0, metadatarefreshCount = 0;
      //int nonSingleHopCount1 = 0, metadatarefreshCount1 = 0;
      Region region = CacheHelper.GetRegion(rname);
      Assert.IsNotNull(region, "DoPRSHFixedPartitionResolverTests: null region.");      
      Util.Log("Inside DoPRSHFixedPartitionResolverTests region name is {0} ", region.Name.ToString());             
      for (int i = 0; i < 2000; i++) {
        try {
          CacheableInt32 key = new CacheableInt32(i);
          int val = key.GetHashCode();
          region.Put(key, val);
          Util.Log("Put inside DoPRSHFixedPartitionResolverTests successfull {0} {1}", key, val);
        }
        catch (CacheServerException ex) {
          Util.Log("CSHARPTEST: Put caused networkhop");
          Assert.Fail("Got CacheServerException (0}", ex.Message);
        }
        catch (CacheWriterException ex) {
          Util.Log("CSHARPTEST: Put caused networkhop");
          Assert.Fail("Got CacheWriterException (0}", ex.Message);
        }
        catch (Exception ex) {
          Util.Log("CSHARPTEST: Put caused networkhop");
          Assert.Fail("Got Exception (0}", ex.Message);
        }
      }        
      StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
      StatisticsType type = factory.FindType("RegionStatistics");
      if (type != null) {
        Statistics rStats = factory.FindFirstStatisticsByType(type);
        if (rStats != null) {
          nonSingleHopCount = rStats.GetInt((string)"nonSingleHopCount");
          metadatarefreshCount = rStats.GetInt((string)"metaDataRefreshCount");            
        }
      }
      Util.Log("DoPRSHFixedPartitionResolverTests Puts completed. nonSingleHopCount = {0} & metadatarefreshCount = {1} ", nonSingleHopCount, metadatarefreshCount);
      Assert.AreEqual(nonSingleHopCount, 1, "nonSingleHopCount should be equal to 1");
      Assert.AreEqual(metadatarefreshCount, 1, "metadatarefreshCount should be equal to 1");

      Util.Log("DoPRSHFixedPartitionResolverTests Puts completed.");
      
      for (int i = 0; i < 1000; i++) {
        try {
          CacheableInt32 key = new CacheableInt32(i);
          region.Get(key);
          Util.Log("Get inside DoPRSHFixedPartitionResolverTests successfull {0}", key);
        }
        catch (CacheServerException ex) {
          Util.Log("CSHARPTEST: Get caused networkhop");
          Assert.Fail("Got CacheServerException (0}", ex.Message);
        }
        catch (CacheWriterException ex) {
          Util.Log("CSHARPTEST: Get caused networkhop");
          Assert.Fail("Got CacheWriterException (0}", ex.Message);
        }
        catch (Exception ex) {
          Util.Log("CSHARPTEST: Get caused networkhop");
          Assert.Fail("Got Exception (0}", ex.Message);
        }

        try {
          CacheableInt32 key = new CacheableInt32(i);
          region.Destroy(key);
          Util.Log("Destroy inside DoPRSHFixedPartitionResolverTests successfull {0}", key);
        }
        catch (CacheServerException ex) {
          Util.Log("CSHARPTEST: Destroy caused networkhop");
          Assert.Fail("Got CacheServerException (0}", ex.Message);
        }
        catch (CacheWriterException ex) {
          Util.Log("CSHARPTEST: Destroy caused networkhop");
          Assert.Fail("Got CacheWriterException (0}", ex.Message);
        }
        catch (Exception ex) {
          Util.Log("CSHARPTEST: Destroy caused networkhop");
          Assert.Fail("Got Exception (0}", ex.Message);
        }
      }
      nonSingleHopCount = metadatarefreshCount = 0;
      if (type != null) {
        Statistics rStats = factory.FindFirstStatisticsByType(type);
        if (rStats != null) {
          nonSingleHopCount = rStats.GetInt((string)"nonSingleHopCount");
          metadatarefreshCount = rStats.GetInt((string)"metaDataRefreshCount");            
          Assert.AreEqual(nonSingleHopCount, 1, "nonSingleHopCount should be equal to 1");
          Assert.AreEqual(metadatarefreshCount, 1, "metadatarefreshCount should be equal to 1");
        }
      }
    }

    public void DoPRSHPartitionResolverTasks(ClientBase client1, ClientBase client2, string regionName)
    {
      client1.Call(DoPRSHPartitionResolverPuts, regionName);
      client2.Call(DoPRSHPartitionResolverPuts, regionName);
    }

    public void DoPRSHFixedPartitionResolverTasks(ClientBase client1, string regionName)
    {
      client1.Call(DoPRSHFixedPartitionResolverTests, regionName);      
    }

    public void DoHashCodePuts(ClientBase client1, ClientBase client2, string regionName)
    {
      client1.Call(DoHashPuts);
      client2.Call(DoHashPuts);
    }

    public void DoKeyChecksumPuts()
    {
      Assert.IsNotNull(m_cKeyCksums, "PutKeyChecksums: null checksums array.");
      Assert.IsNotNull(m_region, "PutKeyChecksums: null region.");
      Util.Log("DoKeyChecksumPuts number of keys " + m_cKeyCksums.Length);
      for (int keyIndex = 0; keyIndex < m_cKeyCksums.Length; keyIndex++) {
        m_region.Put(KeyChecksumPrefix + keyIndex,
          (int)m_cKeyCksums[keyIndex]);
      }
      Util.Log("DoKeyChecksumPuts completed for keyType [{0}], valType [{1}].",
        m_cKeys[0].CacheableKey.GetType(), GetValueType());
    }

    public void DoValChecksumPuts()
    {
      Assert.IsNotNull(m_cValCksums, "PutValChecksums: null checksums array.");
      Assert.IsNotNull(m_region, "PutValChecksums: null region.");
      Util.Log("DoValChecksumPuts number of keys " + m_cValCksums.Length);
      for (int keyIndex = 0; keyIndex < m_cValCksums.Length; keyIndex++) {
        m_region.Put(ValChecksumPrefix + keyIndex, (int)m_cValCksums[keyIndex]);
      }
      Util.Log("DoValChecksumPuts completed for keyType [{0}], valType [{1}].",
        m_cKeys[0].CacheableKey.GetType(), GetValueType());
    }

    /// <summary>
    /// Run a query on server for native client to force deserialization
    /// on server and thereby check serialization/deserialization compability
    /// between native clients and java server.
    /// </summary>
    public void DoRunQuery(bool pool)
    {
      Assert.IsNotNull(m_cKeys, "DoGets: null keys array.");
      Assert.IsNotNull(m_region, "DoGets: null region.");

      // for a type that cannot be handled by server, delete these values
      // before next query that will cause problem
      Type valType = GetValueType();
      if (CacheableHelper.IsUnhandledType(m_cValues[0].TypeId)) {
        Util.Log("DoRunQuery: deleting entries with value type {0}", valType);
        for (int keyIndex = 0; keyIndex < m_cKeys.Length; keyIndex++) {
          m_region.Destroy(m_cKeys[keyIndex].CacheableKey);
        }
      }
      else {
        QueryService qs = null;
        if (pool) {
          qs = PoolManager.Find(m_region.Attributes.PoolName).GetQueryService();
        }
        else {
          qs = m_region.Cache.GetQueryService();
        }
        Query qry = qs.NewQuery("SELECT * FROM " + m_region.FullPath);
        ISelectResults results = qry.Execute();
        // not really interested in results but loop through them neverthless
        Util.Log("DoRunQuery: obtained {0} results", results.Size);
        int numResults = 0;
        foreach (IGFSerializable res in results) {
          ++numResults;
        }
        Assert.AreEqual(results.Size, numResults,
          "Expected the number of results to match the size of ISelectResults");
      }
      Util.Log("DoQuery completed for keyType [{0}], valType [{1}].",
        m_cKeys[0].CacheableKey.GetType(), valType);
    }

    public void DoGetsVerify()
    {
      Util.Log("DoGetsVerify: m_cKeys " + m_cKeys.Length);
      Assert.IsNotNull(m_cKeys, "DoGetsVerify: null keys array.");
      Assert.IsNotNull(m_cValues, "DoGetsVerify: null values array.");
      Assert.IsNotNull(m_region, "DoGetsVerify: null region.");

      for (int keyIndex = 0; keyIndex < m_cKeys.Length; keyIndex++) {
        IGFSerializable actualValue = m_region.Get(
          m_cKeys[keyIndex].CacheableKey);

        if (actualValue == null)
          Util.Log("DoGetsVerify value is null");
        else
          Util.Log("DoGetsVerify value is not null " );
        uint cksum = m_cKeys[keyIndex].GetChecksum();
        Util.Log("DoGetsVerify  key clasid " + m_region.Get(KeyChecksumPrefix + keyIndex).ClassId);
        //Util.Log("DoGetsVerify  key type " + m_region.Get(KeyChecksumPrefix + keyIndex).GetType().ToString());
        CacheableInt32 putCksum = m_region.Get(KeyChecksumPrefix + keyIndex)
          as CacheableInt32;
        Assert.IsNotNull(putCksum,
          "DoGetsVerify: Could not find checksum for key at index {0}.",
          keyIndex);
        Assert.AreEqual(cksum, (uint)putCksum.Value,
          "DoGetsVerify: Checksums of the keys at index {0} differ.",
          keyIndex);

        cksum = m_cValues[keyIndex].GetChecksum(actualValue);
        putCksum = m_region.Get(ValChecksumPrefix + keyIndex)
          as CacheableInt32;
        Assert.IsNotNull(putCksum,
          "DoGetsVerify: Could not find checksum for value at index {0}.",
          keyIndex);
        Assert.AreEqual(cksum, (uint)putCksum.Value,
          "DoGetsVerify: Checksums of the values at index {0} differ.",
          keyIndex);

        // Also check in local cache using GetEntry
        Util.Log("DoGetsVerify() key hashcode " + m_cKeys[keyIndex].CacheableKey.GetHashCode() );
        RegionEntry entry = m_region.GetEntry(m_cKeys[keyIndex].CacheableKey);
        
        if (entry != null) {
          try
          {
            cksum = m_cValues[keyIndex].GetChecksum(entry.Value);
          }
          catch (Exception ex)
          {
            Util.Log("DoGetsVerify() hitesh got exception " + ex.Message);
            Util.Log("DoGetsVerify() hitesh get stacktrace " + ex.StackTrace);
            throw ex;
          }
        }
        else {
          cksum = 0;
        }
        Assert.AreEqual(cksum, (uint)putCksum.Value, "DoGetsVerify: " +
          "Checksums of the values at index {0} differ using GetEntry.",
          keyIndex);
      }
      Util.Log("DoGetsVerify completed for keyType [{0}], valType [{1}].",
        m_cKeys[0].CacheableKey.GetType(), GetValueType());
    }

    public void DoGets()
    {
      Assert.IsNotNull(m_cKeys, "DoGets: null keys array.");
      Assert.IsNotNull(m_region, "DoGets: null region.");

      for (int keyIndex = 0; keyIndex < m_cKeys.Length; keyIndex++) {
        IGFSerializable actualValue = m_region.Get(
          m_cKeys[keyIndex].CacheableKey);
        if (actualValue == null) {
          Assert.AreEqual(GemFireClassIds.CacheableNullString,
            m_cValues[keyIndex].TypeId, "Only null string should return a " +
            "null object");
        }
      }
      Util.Log("DoGets completed for keyType [{0}], valType [{1}].",
        m_cKeys[0].CacheableKey.GetType(), GetValueType());
    }

    public void PutGetSteps(ClientBase client1, ClientBase client2,
      string regionName, bool verifyGets, bool runQuery, bool pool)
    {
      if (verifyGets) {
        client1.Call(DoPuts);
        client1.Call(DoKeyChecksumPuts);
        client1.Call(DoValChecksumPuts);
        client2.Call(DoGetsVerify);
        InvalidateRegion(regionName, client1);
        if (runQuery) {
          // run a query for ThinClient regions to check for deserialization
          // compability on server
          client1.Call(DoRunQuery, pool);
        }
        client2.Call(DoPuts);
        client2.Call(DoKeyChecksumPuts);
        client2.Call(DoValChecksumPuts);
        client1.Call(DoGetsVerify);
      }
      else {
        client1.Call(DoPuts);
        client2.Call(DoGets);
        InvalidateRegion(regionName, client1);
        client2.Call(DoPuts);
        client1.Call(DoGets);
      }
      // this query invocation is primarily to delete the entries that cannot
      // be deserialized by the server
      if (runQuery) {
        client1.Call(DoRunQuery, pool);
      }
    }

    public void InvalidateRegion(string regionName, params ClientBase[] clients)
    {
      if (clients != null) {
        foreach (ClientBase client in clients) {
          client.Call(CacheHelper.InvalidateRegion, regionName, true, true);
        }
      }
    }

    public void TestAllKeyValuePairs(ClientBase client1, ClientBase client2,
      string regionName, bool runQuery, bool pool)
    {
      ICollection<UInt32> registeredKeyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> registeredValueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();

      client1.Call(CacheableHelper.RegisterBuiltins);
      client2.Call(CacheableHelper.RegisterBuiltins);

      foreach (UInt32 keyTypeId in registeredKeyTypeIds) {
        int numKeys;
        client1.Call(InitKeys, out numKeys, keyTypeId, NumKeys, KeySize);
        client2.Call(InitKeys, out numKeys, keyTypeId, NumKeys, KeySize);
        Type keyType = CacheableWrapperFactory.GetTypeForId(keyTypeId);

        foreach (UInt32 valueTypeId in registeredValueTypeIds) {
          client1.Call(InitValues, valueTypeId, numKeys, ValueSize);
          client2.Call(InitValues, valueTypeId, numKeys, ValueSize);
          Type valueType = CacheableWrapperFactory.GetTypeForId(valueTypeId);

          Util.Log("Starting gets/puts with keyType '{0}' and valueType '{1}'",
            keyType.Name, valueType.Name);
          StartTimer();
          Util.Log("Running warmup task which verifies the puts.");
          PutGetSteps(client1, client2, regionName, true, runQuery, pool);
          Util.Log("End warmup task.");
          LogTaskTiming(client1,
            string.Format("Region:{0},Key:{1},Value:{2},KeySize:{3},ValueSize:{4},NumOps:{5}",
            regionName, keyType.Name, valueType.Name, KeySize, ValueSize, 4 * numKeys),
            4 * numKeys);

          InvalidateRegion(regionName, client1, client2);

        }
      }
    }

    public void TestAllKeys(ClientBase client1, ClientBase client2, string regionName)
    {
      ICollection<UInt32> registeredKeyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> registeredValueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();

      client1.Call(CacheableHelper.RegisterBuiltins);
      client2.Call(CacheableHelper.RegisterBuiltins);

      foreach (UInt32 keyTypeId in registeredKeyTypeIds) {
        int numKeys;
        client1.Call(InitKeys, out numKeys, keyTypeId, NumKeys, KeySize);
        client2.Call(InitKeys, out numKeys, keyTypeId, NumKeys, KeySize);
        Type keyType = CacheableWrapperFactory.GetTypeForId(keyTypeId);
        StartTimer();
        Util.Log("Running warmup task which verifies the puts.");
        DoHashCodePuts(client1, client2, regionName);
      }
    }

    #endregion
  }
}
