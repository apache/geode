//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using GemStone.GemFire.DUnitFramework;

  class EventTaskQueue : IDisposable
  {
    #region Private members and classes

#if _NO_QUEUE
    // No members.
#else
    private class QueuedEventTask
    {
      public string m_taskName;
      public Delegate m_processMethod;
      public object[] m_paramList;

      public QueuedEventTask(string taskName, Delegate processMethod, object[] paramList)
      {
        m_taskName = taskName;
        m_processMethod = processMethod;
        m_paramList = paramList;
      }

      public void Process()
      {
        try
        {
          m_processMethod.DynamicInvoke(m_paramList);
        }
        catch (Exception ex)
        {
          Util.Log("########## Exception processing queued task [{0}] ##########", m_taskName);
          Util.Log(ex.ToString());
          Util.Log("############################ End Exception ###########################");
        }
      }
    }

    private Thread m_thread;
    private Queue<QueuedEventTask> m_queue;
    private bool m_shutdown;

    private QueuedEventTask Get()
    {
      lock (((ICollection)m_queue).SyncRoot)
      {
        if (m_queue.Count > 0)
        {
          return m_queue.Dequeue();
        }
      }
      return null;
    }

    private void Process()
    {
      QueuedEventTask task;
      while (!m_shutdown)
      {
        task = Get();
        if (task != null)
        {
          task.Process();
        }
        else
        {
          Thread.Sleep(50);
        }
      }
    }

#endif

    #endregion

    public EventTaskQueue()
    {
#if _NO_QUEUE
      // Nothing.
#else
      m_queue = new Queue<QueuedEventTask>();
      m_shutdown = false;
      m_thread = new Thread(new ThreadStart(Process));
      m_thread.Start();
#endif
    }

    public void Put(string taskName, Delegate processMethod, params object[] paramList)
    {
      QueuedEventTask task = new QueuedEventTask(taskName, processMethod, paramList);
#if _NO_QUEUE
      task.Process();
#else
      lock (((ICollection)m_queue).SyncRoot)
      {
        m_queue.Enqueue(task);
      }
#endif
    }

#if _NO_QUEUE
    // Nothing.
#else
    public string Get(out Delegate processMethod, out object[] paramList)
    {
      QueuedEventTask task = Get();
      if (task != null)
      {
        processMethod = task.m_processMethod;
        paramList = task.m_paramList;
        return task.m_taskName;
      }
      processMethod = null;
      paramList = null;
      return null;
    }

    public void ShutDown()
    {
      m_shutdown = true;
      m_thread.Join();
    }

    public void Kill()
    {
      m_shutdown = true;
      if (!m_thread.Join(1000))
      {
        m_thread.Abort();
      }
    }

#endif

    #region IDisposable Members

    public void Dispose()
    {
#if _NO_QUEUE
      // Nothing to be done.
#else
      ShutDown();
      GC.SuppressFinalize(this);
#endif
    }

    #endregion

#if _NO_QUEUE
      // Nothing to be done.
#else
    ~EventTaskQueue()
    {
      Kill();
    }
#endif
  }

  static class TraderFailed
  {
    public static bool Fail = false;
  }

  delegate void TraderDelegate(CacheableKey key, Serializable value);

  class TraderForwarder : ICacheListener, IDisposable
  {
    private Region m_destRegion;
    private EventTaskQueue m_queue;

    public TraderForwarder(Region region)
    {
#if _EVENT_LOG
      Util.Log("Created forwarder to region {0}", region.Name);
#endif
      m_destRegion = region;
      m_queue = new EventTaskQueue();
    }

    public void CreateOrUpdate(CacheableKey key, Serializable newValue)
    {
#if _EVENT_LOG
      Util.Log("forwarding.");
#endif
      try
      {
        if (m_destRegion == null)
        {
          Util.Log("IllegalState, destRegion == null");
          return;
        }
        if (newValue == null)
        {
          Util.Log("event called with null value... unexpected..");
        }
        m_destRegion.Put(key, newValue);
      }
      catch (Exception ex)
      {
        Util.Log("Exception while forwarding key {0} for region {1}", key, m_destRegion.Name);
        Util.Log(ex.ToString());
        TraderFailed.Fail = true;
      }
    }

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent ev)
    {
#if _EVENT_LOG
      Util.Log("received something.");
#endif
      TraderDelegate processMethod = CreateOrUpdate;
      m_queue.Put("TraderSwapper: AfterCreate for region " +
        ev.Region.Name, (Delegate)processMethod, ev.Key, ev.NewValue);
    }

    public virtual void AfterUpdate(EntryEvent ev)
    {
#if _EVENT_LOG
      Util.Log("received something.");
#endif
      TraderDelegate processMethod = CreateOrUpdate;
      m_queue.Put("TraderSwapper: AfterUpdate for region " +
        ev.Region.Name, (Delegate)processMethod, ev.Key, ev.NewValue);
    }

    public virtual void AfterDestroy(EntryEvent ev) { }

    public virtual void AfterInvalidate(EntryEvent ev) { }

    public virtual void AfterRegionDestroy(RegionEvent ev) { }

    public virtual void AfterRegionClear(RegionEvent ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent ev) { }

    public virtual void AfterRegionLive(RegionEvent ev) { }

    public virtual void Close(Region region) { }
    public void AfterRegionDisconnected(Region region){ }

    #endregion

    #region IDisposable Members

    public void Dispose()
    {
      m_queue.Dispose();
    }

    #endregion
  }

  class TraderSwapper : ICacheListener, IDisposable
  {
    private Region m_srcRegion;
    private Region m_destRegion;
    private EventTaskQueue m_queue;
    private bool m_isAck;

    public TraderSwapper(Region srcRegion, Region destRegion)
    {
#if _EVENT_LOG
      Util.Log("Created swapper from {0} to region {1}", srcRegion.Name, destRegion.Name);
#endif
      m_srcRegion = srcRegion;
      m_destRegion = destRegion;
      m_queue = new EventTaskQueue();
      m_isAck = (srcRegion.Attributes.Scope == ScopeType.DistributedAck);
    }

    public void CreateOrUpdate(CacheableKey key, Serializable newValue)
    {
      // It is GemStone's recommendation that CacheListener's refrain from
      // costly operations. With that said, queueing the application's reaction
      // to these events is beyond the scope of this example.
      //

#if _EVENT_LOG
      Util.Log("swapping something.");
#endif
      try
      {
        if (m_destRegion == null)
        {
          Util.Log("IllegalState, dest == null");
          return;
        }
        if (newValue == null)
        {
          Util.Log("event called with null value... unexpected..");
        }
        // supposed to get the old value from the cache, but to track latency, we need the
        // new value... so, we'll get, but then put the newValue on its way back to process A.
        IGFSerializable cachedValue = m_srcRegion.Get(key);
#if _EVENT_LOG
        Util.Log("swapper get succeeded.");
#endif
        int tries = (m_isAck ? 1 : 30);
        while (tries-- > 0)
        {
          if (cachedValue == null)
          {
            Thread.Sleep(10);
            cachedValue = m_srcRegion.Get(key);
          }
        }
        if (cachedValue == null)
        {
          Util.Log("Error, didn't find value in the cache.");
        }
        else
        {
          m_destRegion.Put(key, newValue);
#if _EVENT_LOG
          Util.Log("swapper put succeeded.");
#endif
        }
      }
      catch (Exception ex)
      {
        Util.Log("Exception occured while swapping {0} for region {1}", key, m_destRegion.Name);
        Util.Log(ex.ToString());
        TraderFailed.Fail = true;
      }
    }

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent ev)
    {
#if _EVENT_LOG
      Util.Log("received something.");
#endif
      TraderDelegate processMethod = CreateOrUpdate;
      m_queue.Put("TraderSwapper: AfterCreate for region " +
        ev.Region.Name, (Delegate)processMethod, ev.Key, ev.NewValue);
    }

    public virtual void AfterUpdate(EntryEvent ev)
    {
#if _EVENT_LOG
      Util.Log("received something.");
#endif
      TraderDelegate processMethod = CreateOrUpdate;
      m_queue.Put("TraderSwapper: AfterUpdate for region " +
        ev.Region.Name, (Delegate)processMethod, ev.Key, ev.NewValue);
    }

    public virtual void AfterDestroy(EntryEvent ev) { }

    public virtual void AfterInvalidate(EntryEvent ev) { }

    public virtual void AfterRegionDestroy(RegionEvent ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent ev) { }

    public virtual void AfterRegionLive(RegionEvent ev) { }

    public virtual void Close(Region region) { }
    public void AfterRegionDisconnected(Region region){ }
    #endregion

    #region IDisposable Members

    public void Dispose()
    {
      m_queue.Dispose();
    }

    #endregion
  }

  class TraderReceiveCounter : ICacheListener
  {
    private RefValue<int> m_received;

    public TraderReceiveCounter(RefValue<int> counter)
    {
      m_received = counter;
#if _EVENT_LOG
      Util.Log("Created ReceiveCounter with {0}", counter);
#endif
    }

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent ev)
    {
      m_received.Value++;
#if _EVENT_LOG
      Util.Log("Received create for something; now {0}.", m_received.Value);
#endif
    }

    public virtual void AfterUpdate(EntryEvent ev)
    {
      m_received.Value++;
#if _EVENT_LOG
      Util.Log("Received update for something; now {0}.", m_received.Value);
#endif
    }

    public virtual void AfterDestroy(EntryEvent ev) { }

    public virtual void AfterInvalidate(EntryEvent ev) { }

    public virtual void AfterRegionDestroy(RegionEvent ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent ev) { }

    public virtual void AfterRegionLive(RegionEvent ev) { }

    public virtual void Close(Region region) { }
    public void AfterRegionDisconnected(Region region){ }
    #endregion
  }
}
