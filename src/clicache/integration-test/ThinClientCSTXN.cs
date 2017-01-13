//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  using GemStone.GemFire.Cache.UnitTests.NewAPI;
  using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  #region CSTX_COMMENTED - transaction listener and writer are disabled for now
  /* 
  class CSTXListener<TKey, TVal> : TransactionListenerAdapter<TKey, TVal>
  {
    public CSTXListener(string cacheName)
    {
      m_cacheName = cacheName; 
    }
    public override void AfterCommit(TransactionEvent<TKey, TVal> te)
    {
      if (te.Cache.Name != m_cacheName)
        incorrectCacheName = true; 
        
      afterCommitEvents++;
      afterCommitKeyEvents += te.Events.Length;
    }

    public override void AfterFailedCommit(TransactionEvent<TKey, TVal> te)
    {
      if (te.Cache.Name != m_cacheName)
        incorrectCacheName = true;

      afterFailedCommitEvents++;
      afterFailedCommitKeyEvents += te.Events.Length;
    }

    public override void AfterRollback(TransactionEvent<TKey, TVal> te)
    {
      if (te.Cache.Name != m_cacheName)
        incorrectCacheName = true;

      afterRollbackEvents++;
      afterRollbackKeyEvents += te.Events.Length;
    }

    public override void Close()
    {
      closeEvent++;
    }
    
    public void ShowTallies()
    {
      Util.Log("CSTXListener state: (afterCommitEvents = {0}, afterRollbackEvents = {1}, afterFailedCommitEvents = {2}, afterCommitRegionEvents = {3}, afterRollbackRegionEvents = {4}, afterFailedCommitRegionEvents = {5}, closeEvent = {6})",
        afterCommitEvents, afterRollbackEvents, afterFailedCommitEvents, afterCommitKeyEvents, afterRollbackKeyEvents, afterFailedCommitKeyEvents, closeEvent);
    }
    
    public int AfterCommitEvents { get { return afterCommitEvents; } }
    public int AfterRollbackEvents { get { return afterRollbackEvents; } }
    public int AfterFailedCommitEvents { get { return afterFailedCommitEvents; } }
    public int AfterCommitKeyEvents { get { return afterCommitKeyEvents; } }
    public int AfterRollbackKeyEvents { get { return afterRollbackKeyEvents; } }
    public int AfterFailedCommitKeyEvents { get { return afterFailedCommitKeyEvents; } }
    public int CloseEvent { get { return closeEvent; } }
    public bool IncorrectCacheName { get { return incorrectCacheName; } }
    
    private int afterCommitEvents = 0;

    private int afterRollbackEvents = 0;
    private int afterFailedCommitEvents = 0;
    
    private int afterCommitKeyEvents = 0;
    private int afterRollbackKeyEvents = 0;
    private int afterFailedCommitKeyEvents = 0;
    private int closeEvent = 0;
    
    private string m_cacheName = null;

    private bool incorrectCacheName = false;

    
  }
  class CSTXWriter<TKey, TVal> : TransactionWriterAdapter<TKey, TVal>
  {
    public CSTXWriter(string cacheName, string instanceName)
    {
      m_instanceName = instanceName;
      m_cacheName = cacheName; 
    }
    
    public override void BeforeCommit(TransactionEvent<TKey, TVal> te)
    {
      if (te.Cache.Name != m_cacheName)
        incorrectCacheName = true;

      beforeCommitEvents++;
      beforeCommitKeyEvents += te.Events.Length; 
    }
    public int BeforeCommitEvents { get { return beforeCommitEvents; } }
    public int BeforeCommitKeyEvents { get { return beforeCommitKeyEvents; } }
    public string InstanceName { get { return m_instanceName; } }
    public bool IncorrectCacheName { get { return incorrectCacheName; } }
    public void ShowTallies()
    {
      Util.Log("CSTXWriter state: (beforeCommitEvents = {0}, beforeCommitRegionEvents = {1}, instanceName = {2})",
        beforeCommitEvents, beforeCommitKeyEvents, m_instanceName);
    }
    private int beforeCommitEvents = 0;
    private int beforeCommitKeyEvents = 0;
    private string m_cacheName = null;
    private string m_instanceName;
    
    private bool incorrectCacheName = false;
    
  }
  */
  #endregion

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  public class ThinClientCSTX : ThinClientRegionSteps
  {
    #region CSTX_COMMENTED - transaction listener and writer are disabled for now
    /*private CSTXWriter<object, object> m_writer1;
    private CSTXWriter<object, object> m_writer2;
    private CSTXListener<object, object> m_listener1;
    private CSTXListener<object, object> m_listener2;*/
    #endregion

    RegionOperation o_region1;
    RegionOperation o_region2;
    private TallyListener<object, object> m_listener;
    private static string[] cstxRegions = new string[] { "cstx1", "cstx2", "cstx3" };
    private UnitProcess m_client1;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      return new ClientBase[] { m_client1 };
    }

    public void CreateRegion(string regionName, string locators, bool listener)
    {
      if (listener)
        m_listener = new TallyListener<object, object>();
      else
        m_listener = null;

      Region region = null;
      region = CacheHelper.CreateTCRegion_Pool<object, object>(regionName, false, false,
        m_listener, locators, "__TESTPOOL1_", false);

    }

    #region CSTX_COMMENTED - transaction listener and writer are disabled for now
    /*
    public void ValidateCSTXListenerWriter()
    {
      Util.Log("tallies for listener 1");
      m_listener1.ShowTallies();
      Util.Log("tallies for writer 1");
      m_writer1.ShowTallies();
      Util.Log("tallies for listener 2");
      m_listener2.ShowTallies();
      Util.Log("tallies for writer 2");
      m_writer2.ShowTallies();
      
       // listener 1
      Assert.AreEqual(4, m_listener1.AfterCommitEvents, "Should be 4");
      Assert.AreEqual(14, m_listener1.AfterCommitKeyEvents, "Should be 14");
      Assert.AreEqual(0, m_listener1.AfterFailedCommitEvents, "Should be 0");
      Assert.AreEqual(0, m_listener1.AfterFailedCommitKeyEvents, "Should be 0");
      Assert.AreEqual(2, m_listener1.AfterRollbackEvents, "Should be 2");
      Assert.AreEqual(6, m_listener1.AfterRollbackKeyEvents, "Should be 6");
      Assert.AreEqual(1, m_listener1.CloseEvent, "Should be 1");
      Assert.AreEqual(false, m_listener1.IncorrectCacheName, "Incorrect cache name in the events");
      
      // listener 2
      Assert.AreEqual(2, m_listener2.AfterCommitEvents, "Should be 2");
      Assert.AreEqual(6, m_listener2.AfterCommitKeyEvents, "Should be 6");
      Assert.AreEqual(0, m_listener2.AfterFailedCommitEvents, "Should be 0");
      Assert.AreEqual(0, m_listener2.AfterFailedCommitKeyEvents, "Should be 0");
      Assert.AreEqual(2, m_listener2.AfterRollbackEvents, "Should be 2");
      Assert.AreEqual(6, m_listener2.AfterRollbackKeyEvents, "Should be 6");
      Assert.AreEqual(1, m_listener2.CloseEvent, "Should be 1");
      Assert.AreEqual(false, m_listener2.IncorrectCacheName, "Incorrect cache name in the events");
      
      // writer 1 
      Assert.AreEqual(3, m_writer1.BeforeCommitEvents, "Should be 3");
      Assert.AreEqual(10, m_writer1.BeforeCommitKeyEvents, "Should be 10");
      Assert.AreEqual(false, m_writer1.IncorrectCacheName, "Incorrect cache name in the events");
      
      // writer 2
      Assert.AreEqual(1, m_writer2.BeforeCommitEvents, "Should be 1");
      Assert.AreEqual(4, m_writer2.BeforeCommitKeyEvents, "Should be 4");
      Assert.AreEqual(false, m_writer2.IncorrectCacheName, "Incorrect cache name in the events");
    }
    */
    #endregion
    public void ValidateListener()
    {
      o_region1 = new RegionOperation(cstxRegions[2]);
      CacheHelper.CSTXManager.Begin();
      o_region1.Region.Put("key3", "value1", null);
      o_region1.Region.Put("key4", "value2", null);
      o_region1.Region.Remove("key4");
      CacheHelper.CSTXManager.Commit();
      // server is conflating the events on the same key hence only 1 create
      Assert.AreEqual(1, m_listener.Creates, "Should be 1 creates");
      Assert.AreEqual(1, m_listener.Destroys, "Should be 1 destroys");

      CacheHelper.CSTXManager.Begin();
      o_region1.Region.Put("key1", "value1", null);
      o_region1.Region.Put("key2", "value2", null);
      o_region1.Region.Invalidate("key1");
      o_region1.Region.Invalidate("key3");
      CacheHelper.CSTXManager.Commit();

      // server is conflating the events on the same key hence only 1 invalidate
      Assert.AreEqual(3, m_listener.Creates, "Should be 3 creates");
      Assert.AreEqual(1, m_listener.Invalidates, "Should be 1 invalidates");

    }
    public void SuspendResumeRollback()
    {
      o_region1 = new RegionOperation(cstxRegions[0]);
      o_region2 = new RegionOperation(cstxRegions[1]);

      CacheHelper.CSTXManager.Begin();
      o_region1.PutOp(1, null);

      o_region2.PutOp(1, null);


      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(CacheHelper.CSTXManager.TransactionId), false, "Transaction should not be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(CacheHelper.CSTXManager.TransactionId), true, "Transaction should exist");
      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be two values in the region before commit");
      Assert.AreEqual(2, o_region2.Region.Keys.Count, "There should be two values in the region before commit");

      TransactionId tid = CacheHelper.CSTXManager.Suspend();

      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(tid), true, "Transaction should be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(tid), true, "Transaction should exist");
      Assert.AreEqual(0, o_region1.Region.Keys.Count, "There should be 0 values in the region after suspend");
      Assert.AreEqual(0, o_region2.Region.Keys.Count, "There should be 0 values in the region after suspend");


      CacheHelper.CSTXManager.Resume(tid);
      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(tid), false, "Transaction should not be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(tid), true, "Transaction should exist");
      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be two values in the region before commit");
      Assert.AreEqual(2, o_region2.Region.Keys.Count, "There should be two values in the region before commit");

      o_region2.PutOp(2, null);

      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be four values in the region before commit");
      Assert.AreEqual(4, o_region2.Region.Keys.Count, "There should be four values in the region before commit");

      CacheHelper.CSTXManager.Rollback();
      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(tid), false, "Transaction should not be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(tid), false, "Transaction should NOT exist");
      Assert.AreEqual(CacheHelper.CSTXManager.TryResume(tid), false, "Transaction should not be resumed");
      Assert.AreEqual(CacheHelper.CSTXManager.TryResume(tid, 3000), false, "Transaction should not be resumed");
      Assert.AreEqual(0, o_region1.Region.Keys.Count, "There should be 0 values in the region after rollback");
      Assert.AreEqual(0, o_region2.Region.Keys.Count, "There should be 0 values in the region after rollback");
      bool resumeEx = false;
      try
      {
        CacheHelper.CSTXManager.Resume(tid);
      }
      catch (IllegalStateException)
      {
        resumeEx = true;
      }
      Assert.AreEqual(resumeEx, true, "The transaction should not be resumed");

    }

    public void SuspendResumeInThread()
    {
      AutoResetEvent txEvent = new AutoResetEvent(false);
      AutoResetEvent txIdUpdated = new AutoResetEvent(false);
      SuspendTransactionThread susObj = new SuspendTransactionThread(false, txEvent, txIdUpdated);
      Thread susThread = new Thread(new ThreadStart(susObj.ThreadStart));
      susThread.Start();
      txIdUpdated.WaitOne();

      ResumeTransactionThread resObj = new ResumeTransactionThread(susObj.Tid, false, false, txEvent);
      Thread resThread = new Thread(new ThreadStart(resObj.ThreadStart));
      resThread.Start();

      susThread.Join();
      resThread.Join();
      Assert.AreEqual(resObj.IsFailed, false, resObj.Error);

      susObj = new SuspendTransactionThread(false, txEvent, txIdUpdated);
      susThread = new Thread(new ThreadStart(susObj.ThreadStart));
      susThread.Start();
      txIdUpdated.WaitOne();

      resObj = new ResumeTransactionThread(susObj.Tid, true, false, txEvent);
      resThread = new Thread(new ThreadStart(resObj.ThreadStart));
      resThread.Start();

      susThread.Join();
      resThread.Join();
      Assert.AreEqual(resObj.IsFailed, false, resObj.Error);


      susObj = new SuspendTransactionThread(true, txEvent, txIdUpdated);
      susThread = new Thread(new ThreadStart(susObj.ThreadStart));
      susThread.Start();
      txIdUpdated.WaitOne();

      resObj = new ResumeTransactionThread(susObj.Tid, false, true, txEvent);
      resThread = new Thread(new ThreadStart(resObj.ThreadStart));
      resThread.Start();

      susThread.Join();
      resThread.Join();
      Assert.AreEqual(resObj.IsFailed, false, resObj.Error);


      susObj = new SuspendTransactionThread(true, txEvent, txIdUpdated);
      susThread = new Thread(new ThreadStart(susObj.ThreadStart));
      susThread.Start();

      txIdUpdated.WaitOne();
      resObj = new ResumeTransactionThread(susObj.Tid, true, true, txEvent);
      resThread = new Thread(new ThreadStart(resObj.ThreadStart));
      resThread.Start();

      susThread.Join();
      resThread.Join();
      Assert.AreEqual(resObj.IsFailed, false, resObj.Error);

    }
    public void SuspendResumeCommit()
    {
      o_region1 = new RegionOperation(cstxRegions[0]);
      o_region2 = new RegionOperation(cstxRegions[1]);

      CacheHelper.CSTXManager.Begin();
      o_region1.PutOp(1, null);

      o_region2.PutOp(1, null);


      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(CacheHelper.CSTXManager.TransactionId), false, "Transaction should not be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(CacheHelper.CSTXManager.TransactionId), true, "Transaction should exist");
      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be two values in the region before commit");
      Assert.AreEqual(2, o_region2.Region.Keys.Count, "There should be two values in the region before commit");

      TransactionId tid = CacheHelper.CSTXManager.Suspend();

      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(tid), true, "Transaction should be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(tid), true, "Transaction should exist");
      Assert.AreEqual(0, o_region1.Region.Keys.Count, "There should be 0 values in the region after suspend");
      Assert.AreEqual(0, o_region2.Region.Keys.Count, "There should be 0 values in the region after suspend");


      CacheHelper.CSTXManager.Resume(tid);
      Assert.AreEqual(CacheHelper.CSTXManager.TryResume(tid), false, "The transaction should not have been resumed again.");
      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(tid), false, "Transaction should not be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(tid), true, "Transaction should exist");
      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be two values in the region before commit");
      Assert.AreEqual(2, o_region2.Region.Keys.Count, "There should be two values in the region before commit");

      o_region2.PutOp(2, null);

      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be four values in the region before commit");
      Assert.AreEqual(4, o_region2.Region.Keys.Count, "There should be four values in the region before commit");

      CacheHelper.CSTXManager.Commit();
      Assert.AreEqual(CacheHelper.CSTXManager.IsSuspended(tid), false, "Transaction should not be suspended");
      Assert.AreEqual(CacheHelper.CSTXManager.Exists(tid), false, "Transaction should NOT exist");
      Assert.AreEqual(CacheHelper.CSTXManager.TryResume(tid), false, "Transaction should not be resumed");
      Assert.AreEqual(CacheHelper.CSTXManager.TryResume(tid, 3000), false, "Transaction should not be resumed");
      Assert.AreEqual(2, o_region1.Region.Keys.Count, "There should be four values in the region after commit");
      Assert.AreEqual(4, o_region2.Region.Keys.Count, "There should be four values in the region after commit");
      o_region1.DestroyOpWithPdxValue(1, null);
      o_region2.DestroyOpWithPdxValue(2, null);
      bool resumeEx = false;
      try
      {
        CacheHelper.CSTXManager.Resume(tid);
      }
      catch (IllegalStateException)
      {
        resumeEx = true;
      }
      Assert.AreEqual(resumeEx, true, "The transaction should not be resumed");
      Assert.AreEqual(CacheHelper.CSTXManager.Suspend(), null, "The transaction should not be suspended");
    }

    public void CallOp()
    {
      #region CSTX_COMMENTED - transaction listener and writer are disabled for now
      /*
      m_writer1 = new CSTXWriter<object, object>(CacheHelper.DCache.Name, "cstxWriter1");
      m_writer2 = new CSTXWriter<object, object>(CacheHelper.DCache.Name, "cstxWriter2");
      m_listener1 = new CSTXListener<object, object>(CacheHelper.DCache.Name);
      m_listener2 = new CSTXListener<object, object>(CacheHelper.DCache.Name);
      
      CacheHelper.CSTXManager.AddListener<object, object>(m_listener1);
      CacheHelper.CSTXManager.AddListener<object, object>(m_listener2);
      CacheHelper.CSTXManager.SetWriter<object, object>(m_writer1);
      
      // test two listener one writer for commit on two regions
      Util.Log(" test two listener one writer for commit on two regions");
      */
      #endregion

      CacheHelper.CSTXManager.Begin();
      o_region1 = new RegionOperation(cstxRegions[0]);
      o_region1.PutOp(2, null);

      o_region2 = new RegionOperation(cstxRegions[1]);
      o_region2.PutOp(2, null);

      CacheHelper.CSTXManager.Commit();
      //two pdx put as well
      Assert.AreEqual(2 + 2, o_region1.Region.Keys.Count, "Commit didn't put two values in the region");
      Assert.AreEqual(2 + 2, o_region2.Region.Keys.Count, "Commit didn't put two values in the region");

      #region CSTX_COMMENTED - transaction listener and writer are disabled for now
      /*
      Util.Log(" test two listener one writer for commit on two regions - complete");
      //////////////////////////////////
      
      // region test two listener one writer for commit on one region
      Util.Log(" region test two listener one writer for commit on one region");
      CacheHelper.CSTXManager.Begin();
      o_region1.PutOp(2, null);

      CacheHelper.CSTXManager.Commit();
      Util.Log(" region test two listener one writer for commit on one region - complete");
      //////////////////////////////////
      // test two listener one writer for rollback on two regions
      Util.Log(" test two listener one writer for rollback on two regions");
      */
      #endregion

      CacheHelper.CSTXManager.Begin();
      o_region1.PutOp(2, null);

      o_region2.PutOp(2, null);

      CacheHelper.CSTXManager.Rollback();
      //two pdx put as well
      Assert.AreEqual(2 + 2, o_region1.Region.Keys.Count, "Region has incorrect number of objects");
      Assert.AreEqual(2 + 2, o_region2.Region.Keys.Count, "Region has incorrect number of objects");
      o_region1.DestroyOpWithPdxValue(2, null);
      o_region2.DestroyOpWithPdxValue(2, null);

      #region CSTX_COMMENTED - transaction listener and writer are disabled for now
      /*
      
      Util.Log(" test two listener one writer for rollback on two regions - complete");
      //////////////////////////////////
      
      // test two listener one writer for rollback on on region
      Util.Log(" test two listener one writer for rollback on on region");
      CacheHelper.CSTXManager.Begin();
     
      o_region2.PutOp(2, null);

      CacheHelper.CSTXManager.Rollback();
      Util.Log(" test two listener one writer for rollback on on region - complete");
      //////////////////////////////////
      
      // test remove listener
      Util.Log(" test remove listener");
      CacheHelper.CSTXManager.RemoveListener<object, object>(m_listener2);
      
      CacheHelper.CSTXManager.Begin();
      o_region1.PutOp(2, null);

      o_region2.PutOp(2, null);

      CacheHelper.CSTXManager.Commit();
      Util.Log(" test remove listener - complete" );
      //////////////////////////////////
      
      // test GetWriter
      Util.Log("test GetWriter");
      CSTXWriter<object, object> writer = (CSTXWriter<object, object>)CacheHelper.CSTXManager.GetWriter<object, object>();
      Assert.AreEqual(writer.InstanceName, m_writer1.InstanceName, "GetWriter is not returning the object set by SetWriter");
      Util.Log("test GetWriter - complete");
      //////////////////////////////////
      
      // set a different writer 
      Util.Log("set a different writer");
      CacheHelper.CSTXManager.SetWriter<object, object>(m_writer2);
      
      CacheHelper.CSTXManager.Begin();
      o_region1.PutOp(2, null);

      o_region2.PutOp(2, null);

      CacheHelper.CSTXManager.Commit();
      Util.Log("set a different writer - complete");
      //////////////////////////////////
      */
      #endregion
    }

    void runThinClientCSTXTest()
    {
      CacheHelper.SetupJavaServers(true, "client_server_transactions.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);

      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CacheHelper.InitClient);
      Util.Log("Creating two regions in client1");
      m_client1.Call(CreateRegion, cstxRegions[0], CacheHelper.Locators, false);
      m_client1.Call(CreateRegion, cstxRegions[1], CacheHelper.Locators, false);
      m_client1.Call(CreateRegion, cstxRegions[2], CacheHelper.Locators, true);

      m_client1.Call(CallOp);
      m_client1.Call(SuspendResumeCommit);
      m_client1.Call(SuspendResumeRollback);
      m_client1.Call(SuspendResumeInThread);


      m_client1.Call(ValidateListener);

      #region CSTX_COMMENTED - transaction listener and writer are disabled for now
      /*
      m_client1.Call(ValidateCSTXListenerWriter);
      */
      #endregion

      m_client1.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    void runThinClientPersistentTXTest()
    {
      CacheHelper.SetupJavaServers(true, "client_server_persistent_transactions.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, "--J=-Dgemfire.ALLOW_PERSISTENT_TRANSACTIONS=true");

      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CacheHelper.InitClient);
      Util.Log("Creating two regions in client1");
      m_client1.Call(CreateRegion, cstxRegions[0], CacheHelper.Locators, false);

      m_client1.Call(initializePdxSerializer);
      m_client1.Call(doPutGetWithPdxSerializer);

      m_client1.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    //Successful
    [Test]
    public void ThinClientCSTXTest()
    {
      runThinClientCSTXTest();
    }

    [Test]
    public void ThinClientPersistentTXTest()
    {
      runThinClientPersistentTXTest();
    }

    public class SuspendTransactionThread
    {
      public SuspendTransactionThread(bool sleep, AutoResetEvent txevent, AutoResetEvent txIdUpdated)
      {
        m_sleep = sleep;
        m_txevent = txevent;
        m_txIdUpdated = txIdUpdated;
      }
      public void ThreadStart()
      {
        RegionOperation o_region1 = new RegionOperation(cstxRegions[0]);
        RegionOperation o_region2 = new RegionOperation(cstxRegions[1]);
        CacheHelper.CSTXManager.Begin();

        o_region1.PutOp(1, null);
        o_region2.PutOp(1, null);

        m_tid = CacheHelper.CSTXManager.TransactionId;
        m_txIdUpdated.Set();

        if (m_sleep)
        {
          m_txevent.WaitOne();
          Thread.Sleep(5000);
        }

        m_tid = CacheHelper.CSTXManager.Suspend();
      }

      public TransactionId Tid
      {
        get
        {
          return m_tid;
        }
      }

      private TransactionId m_tid = null;
      private bool m_sleep = false;
      private AutoResetEvent m_txevent = null;
      AutoResetEvent m_txIdUpdated = null;
    }

    public class ResumeTransactionThread
    {
      public ResumeTransactionThread(TransactionId tid, bool isCommit, bool tryResumeWithSleep, AutoResetEvent txevent)
      {
        m_tryResumeWithSleep = tryResumeWithSleep;
        m_txevent = txevent;
        m_tid = tid;
        m_isCommit = isCommit;
      }

      public void ThreadStart()
      {
        RegionOperation o_region1 = new RegionOperation(cstxRegions[0]);
        RegionOperation o_region2 = new RegionOperation(cstxRegions[1]);
        if (m_tryResumeWithSleep)
        {
          if (AssertCheckFail(CacheHelper.CSTXManager.IsSuspended(m_tid) == false, "Transaction should not be suspended"))
            return;
        }
        else
        {
          if (AssertCheckFail(CacheHelper.CSTXManager.IsSuspended(m_tid) == true, "Transaction should be suspended"))
            return;
        }
        if (AssertCheckFail(CacheHelper.CSTXManager.Exists(m_tid) == true, "Transaction should exist"))
          return;
        if (AssertCheckFail(0 == o_region1.Region.Keys.Count, "There should be 0 values in the region after suspend"))
          return;
        if (AssertCheckFail(0 == o_region2.Region.Keys.Count, "There should be 0 values in the region after suspend"))
          return;

        if (m_tryResumeWithSleep)
        {
          m_txevent.Set();
          CacheHelper.CSTXManager.TryResume(m_tid, 30000);
        }
        else
          CacheHelper.CSTXManager.Resume(m_tid);

        if (AssertCheckFail(CacheHelper.CSTXManager.IsSuspended(m_tid) == false, "Transaction should not be suspended"))
          return;
        if (AssertCheckFail(CacheHelper.CSTXManager.Exists(m_tid) == true, "Transaction should exist"))
          return;
        if (AssertCheckFail(o_region1.Region.Keys.Count == 2, "There should be two values in the region after suspend"))
          return;
        if (AssertCheckFail(o_region2.Region.Keys.Count == 2, "There should be two values in the region after suspend"))
          return;

        o_region2.PutOp(2, null);

        if (m_isCommit)
        {
          CacheHelper.CSTXManager.Commit();
          if (AssertCheckFail(CacheHelper.CSTXManager.IsSuspended(m_tid) == false, "Transaction should not be suspended"))
            return;
          if (AssertCheckFail(CacheHelper.CSTXManager.Exists(m_tid) == false, "Transaction should NOT exist"))
            return;
          if (AssertCheckFail(CacheHelper.CSTXManager.TryResume(m_tid) == false, "Transaction should not be resumed"))
            return;
          if (AssertCheckFail(CacheHelper.CSTXManager.TryResume(m_tid, 3000) == false, "Transaction should not be resumed"))
            return;
          if (AssertCheckFail(2 == o_region1.Region.Keys.Count, "There should be four values in the region after commit"))
            return;
          if (AssertCheckFail(4 == o_region2.Region.Keys.Count, "There should be four values in the region after commit"))
            return;
          o_region1.DestroyOpWithPdxValue(1, null);
          o_region2.DestroyOpWithPdxValue(2, null);
        }
        else
        {
          CacheHelper.CSTXManager.Rollback();
          if (AssertCheckFail(CacheHelper.CSTXManager.IsSuspended(m_tid) == false, "Transaction should not be suspended"))
            return;
          if (AssertCheckFail(CacheHelper.CSTXManager.Exists(m_tid) == false, "Transaction should NOT exist"))
            return;
          if (AssertCheckFail(CacheHelper.CSTXManager.TryResume(m_tid) == false, "Transaction should not be resumed"))
            return;
          if (AssertCheckFail(CacheHelper.CSTXManager.TryResume(m_tid, 3000) == false, "Transaction should not be resumed"))
            return;
          if (AssertCheckFail(0 == o_region1.Region.Keys.Count, "There should be 0 values in the region after rollback"))
            return;
          if (AssertCheckFail(0 == o_region2.Region.Keys.Count, "There should be 0 values in the region after rollback"))
            return;
        }

      }

      public bool AssertCheckFail(bool cond, String error)
      {
        if (!cond)
        {
          m_isFailed = true;
          m_error = error;
          return true;
        }
        return false;
      }

      public TransactionId Tid
      {
        get { return m_tid; }
      }
      public bool IsFailed
      {
        get { return m_isFailed; }
      }
      public String Error
      {
        get { return m_error; }
      }

      private TransactionId m_tid = null;
      private bool m_tryResumeWithSleep = false;
      private bool m_isFailed = false;
      private String m_error;
      private bool m_isCommit = false;
      private AutoResetEvent m_txevent = null;

    }
    
    public void initializePdxSerializer()
    {
      Serializable.RegisterPdxSerializer(new PdxSerializer());
    }

    public void doPutGetWithPdxSerializer()
    {
      CacheHelper.CSTXManager.Begin();
      o_region1 = new RegionOperation(cstxRegions[0]);
      for (int i = 0; i < 10; i++)
      {
        o_region1.Region[i] = i + 1;
        object ret = o_region1.Region[i];
        o_region1.Region[i + 10] = i + 10;
        ret = o_region1.Region[i + 10];
        o_region1.Region[i + 20] = i + 20;
        ret = o_region1.Region[i + 20];
      }
      CacheHelper.CSTXManager.Commit();
      Util.Log("Region keys count after commit for non-pdx keys = {0} ", o_region1.Region.Keys.Count);
      Assert.AreEqual(30, o_region1.Region.Keys.Count, "Commit didn't put two values in the region");

      CacheHelper.CSTXManager.Begin();
      o_region1 = new RegionOperation(cstxRegions[0]);
      for (int i = 100; i < 110; i++)
      {
        object put = new SerializePdx1(true);
        o_region1.Region[i] = put;                
        put = new SerializePdx2(true);
        o_region1.Region[i + 10] = put;
        put = new SerializePdx3(true, i % 2);
        o_region1.Region[i + 20] = put;        
      }
      CacheHelper.CSTXManager.Commit();

      for (int i = 100; i < 110; i++)
      {
        object put = new SerializePdx1(true);
        object ret = o_region1.Region[i];
        Assert.AreEqual(put, ret);
        put = new SerializePdx2(true);
        ret = o_region1.Region[i + 10];
        Assert.AreEqual(put, ret);
        put = new SerializePdx3(true, i % 2);
        ret = o_region1.Region[i + 20];
        Assert.AreEqual(put, ret);
      }
      Util.Log("Region keys count after pdx-keys commit = {0} ", o_region1.Region.Keys.Count);
      Assert.AreEqual(60, o_region1.Region.Keys.Count, "Commit didn't put two values in the region");      
    }
  }
}
