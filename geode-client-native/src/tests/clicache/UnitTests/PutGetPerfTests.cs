//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  public class PutGetPerfTests : UnitTests
  {
    #region Constants

    private const int MaxKeys = 25000;
    private const bool Mirrored = false;

    #endregion

    private CacheableString[] keysA = null;
    private CacheableString[] keysB = null;
    private CacheableString[] valuesA = null;
    private CacheableString[] valuesB = null;

    private CacheableInt32[] intKeysA = null;
    private CacheableInt32[] intKeysB = null;
    private CacheableInt32[] intValuesA = null;
    private CacheableInt32[] intValuesB = null;

    private UnitProcess m_client1, m_client2, m_client3, m_client4;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      m_client4 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3, m_client4 };
    }

    #region Private functions

    private void InitStringObjects(int num, string prefix,
      ref CacheableString[] strings)
    {
      if (num > 0)
      {
        strings = new CacheableString[num];
        for (int i = 1; i <= num; i++)
        {
          strings[i - 1] = new CacheableString(prefix + i.ToString());
        }
      }
    }

    private void InitIntObjects(int num, int offset, int factor,
      ref CacheableInt32[] ints)
    {
      if (num > 0)
      {
        ints = new CacheableInt32[num];
        for (int i = 1; i <= num; i++)
        {
          ints[i - 1] = new CacheableInt32(offset + i * factor);
        }
      }
    }

    private void Puts(CacheableKey[] keys, Serializable[] values,
      int offset, int numIters, int step)
    {
      if (keys != null && values != null && keys.Length == values.Length)
      {
        int numKeys = keys.Length;
        if (numIters <= 0)
        {
          numIters = numKeys;
        }
        for (int i = offset; i < offset + numIters; i += step)
        {
          CacheHelper.CurrentRegion.Put(keys[i % numKeys], values[i % numKeys]);
        }
      }
    }

    private void Gets(CacheableKey[] keys,
      int offset, int numIters, int step)
    {
      if (keys != null)
      {
        int numKeys = keys.Length;
        if (numIters <= 0)
        {
          numIters = numKeys;
        }
        for (int i = offset; i < offset + numIters; i += step)
        {
          CacheHelper.CurrentRegion.Get(keys[i % numKeys]);
        }
      }
    }

    #endregion

    #region Functions that are invoked by the tests

    public enum RegionOp { Put, Get }
    public enum KeysSelect { KeysA, KeysB, IntKeysA, IntKeysB }

    public void InitKeysValues(int num)
    {
      InitStringObjects(num, "KeysA - ", ref keysA);
      InitStringObjects(num, "KeysB - ", ref keysB);
      InitStringObjects(num, "ValuesA - ", ref valuesA);
      InitStringObjects(num, "ValuesB - ", ref valuesB);

      InitIntObjects(num, 1, 2, ref intKeysA);
      InitIntObjects(num, 0, 2, ref intKeysB);
      InitIntObjects(num, 0, 2, ref intValuesA);
      InitIntObjects(num, 1, 2, ref intValuesB);
    }

    public void RegionOpsAB(RegionOp op, KeysSelect sel,
      int offset, int numIters, int step)
    {
      CacheableKey[] keys = null;
      Serializable[] values = null;
      switch (sel)
      {
        case KeysSelect.KeysA:
          keys = (CacheableKey[])keysA;
          values = (Serializable[])valuesA;
          break;
        case KeysSelect.KeysB:
          keys = (CacheableKey[])keysB;
          values = (Serializable[])valuesB;
          break;
        case KeysSelect.IntKeysA:
          keys = (CacheableKey[])intKeysA;
          values = (Serializable[])intValuesA;
          break;
        case KeysSelect.IntKeysB:
          keys = (CacheableKey[])intKeysB;
          values = (Serializable[])intValuesB;
          break;
      }
      switch (op)
      {
        case RegionOp.Put:
          Puts(keys, values, offset, numIters, step);
          break;
        case RegionOp.Get:
          Gets(keys, offset, numIters, step);
          break;
      }
    }

    public void CheckNumKeys(int numExpected)
    {
      int numKeys = 0;
      string message;
      IGFSerializable[] keys = CacheHelper.CurrentRegion.GetKeys();
      if (keys != null)
      {
        numKeys = keys.Length;
      }
      if (numExpected == 0)
      {
        message = "Region is not empty.";
      }
      else
      {
        message = "Region does not contain the expected number of entries.";
      }
      Assert.AreEqual(numExpected, numKeys, message);
    }

    #endregion

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();

      m_client1.Call(InitKeysValues, MaxKeys);
      m_client2.Call(InitKeysValues, MaxKeys);
      m_client3.Call(InitKeysValues, MaxKeys);
      m_client4.Call(InitKeysValues, MaxKeys);
    }

    private void DoCommonDistribTests(string putTestName, int putIters,
      string failTestName, int failIters, string proc2TestName)
    {
      StartTimer();
      m_client1.Call(RegionOpsAB, RegionOp.Put, KeysSelect.KeysA, 0, putIters, 1);
      LogTaskTiming(m_client1, putTestName, putIters);

      StartTimer();
      m_client1.Call(RegionOpsAB, RegionOp.Get, KeysSelect.KeysB, 0, failIters, 1);
      LogTaskTiming(m_client1, failTestName, failIters);

     
      m_client2.Call(CheckNumKeys, 0);
      StartTimer();
      m_client2.Call(RegionOpsAB, RegionOp.Get, KeysSelect.KeysA, 0, MaxKeys, 1);
      LogTaskTiming(m_client2, proc2TestName, MaxKeys);
      m_client2.Call(CheckNumKeys, MaxKeys);
    }

    [Test]
    public void NoPeers()
    {
      int numIters = 50 * MaxKeys;

      m_client1.Call(CacheHelper.CreateScopeRegion, "LocalOnly", ScopeType.Local, true);

      StartTimer();
      m_client1.Call(RegionOpsAB, RegionOp.Put, KeysSelect.KeysA, 0, numIters, 1);
      LogTaskTiming(m_client1, "LocalPuts", numIters);

      StartTimer();
      m_client1.Call(RegionOpsAB, RegionOp.Get, KeysSelect.KeysA, 0, numIters, 1);
      LogTaskTiming(m_client1, "LocalGets", numIters);

      StartTimer();
      m_client1.Call(RegionOpsAB, RegionOp.Get, KeysSelect.KeysB, 0, numIters, 1);
      LogTaskTiming(m_client1, "LocalFailGets", numIters);

      m_client1.Call(CacheHelper.DestroyRegion, "LocalOnly", false, true);

      m_client1.Call(CacheHelper.CreateScopeRegion, "DistNoPeers", ScopeType.DistributedNoAck, true);

      StartTimer();
      m_client1.Call(RegionOpsAB, RegionOp.Put, KeysSelect.KeysA, 0, numIters, 1);
      LogTaskTiming(m_client1, "DistNoPeersPuts", numIters);

      m_client1.Call(CacheHelper.DestroyRegion, "DistNoPeers", false, true);
    }

    [Test]
    public void NoAck2Proc()
    {
      m_client1.Call(CacheHelper.CreateScopeRegion, "NoAck2Proc", ScopeType.DistributedNoAck, true);
      m_client2.Call(CacheHelper.CreateScopeRegion, "NoAck2Proc", ScopeType.DistributedNoAck, true);

      DoCommonDistribTests("NoAck2ProcPuts", 10 * MaxKeys,
        "NoAck2ProcNetsearchFail", 2 * MaxKeys, "NoAck2ProcNetsearch");

      m_client1.Call(CacheHelper.DestroyRegion, "NoAck2Proc", false, true);
    }

    [Test]
    public void Ack2Proc()
    {
      m_client1.Call(CacheHelper.CreateScopeRegion, "Ack2Proc", ScopeType.DistributedAck, true);
      m_client2.Call(CacheHelper.CreateScopeRegion, "Ack2Proc", ScopeType.DistributedAck, true);

      DoCommonDistribTests("Ack2ProcPuts", 2 * MaxKeys,
        "Ack2ProcNetsearchFail", MaxKeys, "Ack2ProcNetsearch");

      m_client1.Call(CacheHelper.DestroyRegion, "Ack2Proc", false, true);
    }

    [Test]
    public void NoAck3Proc()
    {
      m_client1.Call(CacheHelper.CreateScopeRegion, "NoAck3Proc", ScopeType.DistributedNoAck, true);
      m_client2.Call(CacheHelper.CreateScopeRegion, "NoAck3Proc", ScopeType.DistributedNoAck, true);
      m_client3.Call(CacheHelper.CreateScopeRegion, "NoAck3Proc", ScopeType.DistributedNoAck, true);

      DoCommonDistribTests("NoAck3ProcPuts", 2 * MaxKeys,
        "NoAck3ProcNetsearchFail", MaxKeys, "NoAck3ProcNetsearch");

      m_client1.Call(CacheHelper.DestroyRegion, "NoAck3Proc", false, true);
    }

    [Test]
    public void Ack3Proc()
    {
      m_client1.Call(CacheHelper.CreateScopeRegion, "Ack3Proc", ScopeType.DistributedAck, true);
      m_client2.Call(CacheHelper.CreateScopeRegion, "Ack3Proc", ScopeType.DistributedAck, true);
      m_client3.Call(CacheHelper.CreateScopeRegion, "Ack3Proc", ScopeType.DistributedAck, true);

      DoCommonDistribTests("Ack3ProcPuts", MaxKeys,
        "Ack3ProcNetsearchFail", MaxKeys, "Ack3ProcNetsearch");

      m_client1.Call(CacheHelper.DestroyRegion, "Ack3Proc", false, true);
    }

    [Test]
    public void NoAck4Proc()
    {
      m_client1.Call(CacheHelper.CreateScopeRegion, "NoAck4Proc", ScopeType.DistributedNoAck, true);
      m_client2.Call(CacheHelper.CreateScopeRegion, "NoAck4Proc", ScopeType.DistributedNoAck, true);
      m_client3.Call(CacheHelper.CreateScopeRegion, "NoAck4Proc", ScopeType.DistributedNoAck, true);
      m_client4.Call(CacheHelper.CreateScopeRegion, "NoAck4Proc", ScopeType.DistributedNoAck, true);

      DoCommonDistribTests("NoAck4ProcPuts", MaxKeys,
        "NoAck4ProcNetsearchFail", MaxKeys, "NoAck4ProcNetsearch");

      m_client1.Call(CacheHelper.DestroyRegion, "NoAck4Proc", false, true);
    }

    [Test]
    public void Ack4Proc()
    {
      m_client1.Call(CacheHelper.CreateScopeRegion, "Ack4Proc", ScopeType.DistributedAck, true);
      m_client2.Call(CacheHelper.CreateScopeRegion, "Ack4Proc", ScopeType.DistributedAck, true);
      m_client3.Call(CacheHelper.CreateScopeRegion, "Ack4Proc", ScopeType.DistributedAck, true);
      m_client4.Call(CacheHelper.CreateScopeRegion, "Ack4Proc", ScopeType.DistributedAck, true);

      DoCommonDistribTests("Ack4ProcPuts", MaxKeys,
        "Ack4ProcNetsearchFail", MaxKeys, "Ack4ProcNetsearch");

      m_client1.Call(CacheHelper.DestroyRegion, "Ack4Proc", false, true);
    }
  }
}
