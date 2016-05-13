//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("unicast_only")]
  public class TraderTests : UnitTests
  {
    #region Private members and accessors

    private const int ValSize = 1000;
    private const string FromAName = "dunit_FromA";
    private const string ToAName = "dunit_ToA";
    private const string FromCName = "dunit_FromC";
    private const string ToCName = "dunit_ToC";

    private UnitProcess m_client1, m_client2, m_client3, m_client4;
    private Region m_regionFromA, m_regionToA, m_regionFromC, m_regionToC;
    private RefValue<int> m_receivedCount;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      m_client4 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3, m_client4 };
    }

    #region Functions used by the tests

    public void Close()
    {
      CacheHelper.Close();
      Assert.IsFalse(TraderFailed.Fail, "prior exception during queue processing.");
    }

    public void CheckValue(int batch, RefValue<int> receivedCount, int expected)
    {
      int lastReceivedCount = receivedCount;
      int lastReceivedCountSame = 0;

      while (receivedCount < expected)
      {
        Thread.Sleep(1000); // one second.
        Util.Log("batch[{0}] waiting to receive all sent messages. received: {1}, expected: {2}",
          batch, receivedCount.Value, expected);
        if (lastReceivedCount != receivedCount)
        {
          lastReceivedCount = receivedCount;
          lastReceivedCountSame = 0;
        }
        else if (++lastReceivedCountSame > 60)
        {
          Assert.Fail("batch[{0}] have not received any new entry for the past 1 minute", batch);
        }
      }
      Util.Log("batch[{0}] finished receiving messages. total = {1}", batch, receivedCount);
    }

    public void SendValue(int batch, Region region, bool multicast,
      string value, int begin, int end)
    {
      CacheableString cVal = new CacheableString(value);
      for (int i = begin; i < end; i++)
      {
        region.Put("key" + i.ToString("D05"), cVal);
        if (multicast)
        {
          Thread.Sleep(10);
        }
      }
      Util.Log("finished sending messages for batch[{0}].", batch);
    }

    private Region CreateRegion(string regPrefix, string regSuffix, bool ack,
      bool mirror, bool caching, ICacheListener listener)
    {
      return CacheHelper.CreateILRegion(regPrefix + regSuffix, ack, caching, listener);
    }

    public void MirrorCreate(string regPrefix, bool ack)
    {
      m_regionFromA = CreateRegion(regPrefix, FromAName, ack, true, true, null);
    }

    public void CreateProcBDNoCache(string regPrefix, bool ack)
    {
      ICacheListener listener;

      m_regionToC = CreateRegion(regPrefix, ToCName, ack, false, false, null);
      listener = new TraderForwarder(m_regionToC);
      m_regionFromA = CreateRegion(regPrefix, FromAName, ack, false, false, listener);
      m_regionToA = CreateRegion(regPrefix, ToAName, ack, false, false, null);
      listener = new TraderSwapper(m_regionFromA, m_regionToA);
      m_regionFromC = CreateRegion(regPrefix, FromCName, ack, false, false, listener);
    }

    public void CreateProcC(string regPrefix, bool ack)
    {
      ICacheListener listener;

      m_regionFromC = CreateRegion(regPrefix, FromCName, ack, false, false, null);
      listener = new TraderForwarder(m_regionFromC);
      m_regionToC = CreateRegion(regPrefix, ToCName, ack, false, false, listener);
    }

    public void CreateProcA(string regPrefix, bool ack)
    {
      ICacheListener listener;

      m_regionFromA = CreateRegion(regPrefix, FromAName, ack, false, false, null);
      m_receivedCount = (RefValue<int>)0;
      listener = new TraderReceiveCounter(m_receivedCount);
      m_regionToA = CreateRegion(regPrefix, ToAName, ack, false, false, listener);
    }

    public void DriverProcA()
    {
      // put a bunch in FromA, and expect a bunch to be received by ToA.
      bool multicast = false;
      int expected = 50000;
      int batch_count = 2;
      int begin, end;
      int expectedCount = expected / batch_count;
      Util.Log("Expected={0}", expected);

      string value = new string('A', ValSize);
      for (int j = 0; j < batch_count; j++)
      {
        begin = j * expectedCount;
        end = (j + 1) * expectedCount;
        Util.Log("batch[{0}]: from {1}, to {2}, total {3}",
          j, begin, end, expectedCount);
        SendValue(j, m_regionFromA, multicast, value, begin, end);
        CheckValue(j, m_receivedCount, end);
      }
    }

    #endregion

    public void TraderSteps(string regPrefix, bool ack)
    {
      try
      {
        m_client1.Call(MirrorCreate, regPrefix, ack);
        m_client2.Call(CreateProcBDNoCache, regPrefix, ack);
        m_client3.Call(CreateProcC, regPrefix, ack);
        m_client4.Call(CreateProcA, regPrefix, ack);
        m_client4.Call(DriverProcA);
      }
      finally
      {
        m_client1.Call(CacheHelper.DestroyAllRegions, false);
        m_client2.Call(CacheHelper.DestroyAllRegions, false);
        m_client3.Call(CacheHelper.DestroyAllRegions, false);
        m_client4.Call(CacheHelper.DestroyAllRegions, false);
      }
    }

    [Test]
    public void Ack()
    {
      TraderSteps("Ack_", true);
    }

    [Test]
    public void NoAck()
    {
      TraderSteps("NoAck_", false);
    }
  }
}
