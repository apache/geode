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
  public class AckMixTests : UnitTests
  {
    private TallyListener m_listener = new TallyListener();
    private AckMixRegionWrapper m_regionw;
    private const string RegionName = "ConfusedScope";

    private UnitProcess m_client1, m_client2;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    #region Private functions

    private void Init()
    {
      Properties dsysConfig = new Properties();
      dsysConfig.Insert("ack-wait-threshold", "5");
      CacheHelper.InitConfig(dsysConfig);
    }

    #endregion

    #region Functions called by the tests

    public void CreateRegionNOIL(int clientNum, bool ack)
    {
      Init();
      string ackStr;
      if (ack)
      {
        ackStr = "with-ack";
      }
      else
      {
        ackStr = "no-ack";
      }
      Util.Log("Creating region in client {0}, " + ackStr +
        ", no-mirror, cache, no-interestlist, with-listener", clientNum);
      CacheHelper.CreateILRegion(RegionName, ack, true, m_listener);
      m_regionw = new AckMixRegionWrapper(RegionName);
    }

    //Verify no events received by the process
    public void VerifyNoEvents()
    {
      Util.Log("Verifying TallyListener has received nothing.");
      Assert.AreEqual(0, m_listener.Creates, "Should be no creates");
      Assert.AreEqual(0, m_listener.Updates, "Should be no updates");
      Assert.IsNull(m_listener.LastKey, "Should be no key");
      Assert.IsNull(m_listener.LastValue, "Should be no value");
    }

    public void SendCreate()
    {
      Util.Log("put(1,1) from Client 1 noack");
      m_regionw.Put(1, 1);
      m_listener.ShowTallies();
    }

    //Test cache didn't stored update
    public void TestCreateFromAck()
    {
      Util.Log("test(1, -1) in Client 2 ack");
      m_regionw.Test(1, -1);

      // now define something of our own to test create going the other way.
      Util.Log("put(2,2) from Client 2");
      m_regionw.Put(2, 2);
      Assert.AreEqual(1, m_listener.Creates, "Should be 1 create.");
      // send update from ACK to NOACK
      Util.Log("put(1,3) from Client 2");

      Thread.Sleep(10000); // see if we can drop the incoming create before we define the key.
      m_regionw.Test(1, -1);

      m_regionw.Put(1, 3);
      Assert.AreEqual(2, m_listener.Creates, "Should be 2 creates.");
      m_listener.ShowTallies();
    }

    public void TestCreateFromNoAck()
    {
      Util.Log("test( 2, -1 ) in Client 1");
      m_regionw.Test(2, -1);
      Util.Log("test( 1, 3 ) in Client 1");
      m_regionw.Test(1, 3);

      // Now Updates from Client 1 to Client 2.
      Util.Log("put( 1, 5) from Client 1");
      m_regionw.Put(1, 5);
      m_listener.ShowTallies();
    }

    public void TestUpdateFromAck()
    {
      Util.Log("test( 1, 5 ) in Client 2");
      m_regionw.Test(1, 5, true);
    }

    public void TestEventCount(int clientNum)
    {
      Util.Log("check Client {0} event count.", clientNum);
      m_listener.ShowTallies();
      Assert.AreEqual(2, m_listener.ExpectCreates(2), "Should have been 2 creates.");
      Assert.AreEqual(1, m_listener.ExpectUpdates(1), "Should have been 1 update.");
    }

    public void TestTimeoutSetup()
    {
      Util.Log("creating after timeout key to verify.");
      m_regionw.Put(40000, 1);
      m_regionw.SetupTimeout(m_listener);
    }

    public void TestPutTimeout(int newVal)
    {
      Util.Log("checking that timeout works.");
      try
      {
        m_listener.IgnoreTimeout = true;
        m_regionw.RequestTimeout();
        Assert.Fail("Should have thrown TimeoutException.");
      }
      catch (TimeoutException)
      {
        // good.. expected this... now sleep a bit and move on.
        Util.Log("Expected: Received TimeoutException ( good news. )");
        m_listener.IgnoreTimeout = false;
        Thread.Sleep(10000); // other process should be clear by now.
        m_regionw.Put(40000, newVal); // Make sure we succeed with this next put.
        Util.Log("Sent update to key 40000.");
      }
      catch (Exception ex)
      {
        Util.Log(ex.ToString());
        Assert.Fail(ex.Message);
      }
    }

    public void TestAfterPutTimeout(int newVal)
    {
      Util.Log("verifing values made it through from TestTimeout.");
      m_regionw.CheckTimeout();
      m_regionw.Test(40000, newVal);
    }

    #endregion

    [Test]
    public void AckMix()
    {
      m_client1.Call(CreateRegionNOIL, 1, false);
      m_client2.Call(CreateRegionNOIL, 2, true);

      m_client1.Call(VerifyNoEvents);
      m_client2.Call(VerifyNoEvents);

      m_client1.Call(SendCreate);
      m_client2.Call(TestCreateFromAck);

      m_client1.Call(TestCreateFromNoAck);
      m_client2.Call(TestUpdateFromAck);

      m_client1.Call(TestEventCount, 1);
      m_client2.Call(TestEventCount, 2);

      m_client1.Call(TestTimeoutSetup);
      m_client2.Call(TestPutTimeout, 4000);

      m_client1.Call(TestAfterPutTimeout, 4000);
      m_client2.Call(TestPutTimeout, 5000);

      m_client1.Call(TestAfterPutTimeout, 5000);
    }
  }

  class AckMixRegionWrapper : RegionWrapper
  {
    private bool m_timeoutUpdate;

    private static CacheableString timeoutKey =
      new CacheableString("timeout");

    public AckMixRegionWrapper(string name)
      : base(name)
    {
      m_timeoutUpdate = false;
    }

    public void RequestTimeout()
    {
      string timeoutValue = "timeout";
      if (m_timeoutUpdate)
      {
        timeoutValue = "timeoutUpdate";
      }
      DateTime start = DateTime.Now;
      TimeSpan span;
      try
      {
        m_region.Put(timeoutKey, timeoutValue);
        span = DateTime.Now - start;
        Util.Log("put took {0} millis value({1})", span.TotalMilliseconds, timeoutValue);
      }
      catch
      {
        span = DateTime.Now - start;
        Util.Log("put took {0} millis value({1})", span.TotalMilliseconds, timeoutValue);
        m_timeoutUpdate = true;
        throw;
      }
    }

    public void CheckTimeout()
    {
      string timeoutValue = "timeout";
      if (m_timeoutUpdate)
      {
        timeoutValue = "timeoutUpdate";
      }
      CacheableString val = m_region.Get(timeoutKey) as CacheableString;
      Assert.AreEqual(timeoutValue, val.ToString());
      m_timeoutUpdate = true;
    }

    public void SetupTimeout(TallyListener listener)
    {
      listener.IgnoreTimeout = true;
      m_region.Put(timeoutKey, timeoutKey);
      listener.IgnoreTimeout = false;
    }
  }
}
