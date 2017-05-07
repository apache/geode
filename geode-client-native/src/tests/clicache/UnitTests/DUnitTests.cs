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
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class DUnitTests : UnitTests
  {
    #region Private members

    private int m_val;

    #endregion

    #region Constants

    private const int maxVal1 = 1000;
    private const int maxVal2 = 10000;
    public const string testKey = "incrdecr";

    #endregion

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    #region Utility and other functions for tests

    private void ThreadIncrement()
    {
      Interlocked.Increment(ref m_val);
    }

    private void ThreadDecrement()
    {
      Interlocked.Decrement(ref m_val);
    }

    private void ThreadCheckVal(int checkVal)
    {
      Assert.AreEqual(checkVal, m_val);
    }

    private void ConcurrencyTestStart(object execFn)
    {
      if (execFn is UnitFnMethod)
      {
        UnitFnMethod fn = execFn as UnitFnMethod;
        using (UnitThread ut = new UnitThread())
        {
          for (int i = 1; i <= maxVal2; i++)
          {
            ut.Call(fn);
          }
        }
      }
    }

    public void ProcessIncrement()
    {
      Util.BBIncrement(string.Empty, testKey);
    }

    public void ProcessDecrement()
    {
      Util.BBDecrement(string.Empty, testKey);
    }

    public void ProcessCheckVal(int checkVal)
    {
      int currVal = (int)Util.BBGet(string.Empty, testKey);
      Assert.AreEqual(checkVal, currVal);
    }

    #endregion

    /// <summary>
    /// This test increments an integer using two threads and checks the result.
    /// </summary>
    [Test]
    public void ThreadIncrDecr()
    {
      m_val = 0;
      using (UnitThread ut1 = new UnitThread())
      {
        using (UnitThread ut2 = new UnitThread())
        {
          for (int i = 1; i <= maxVal1; i++)
          {
            ut1.Call(ThreadIncrement);
            ut2.Call(ThreadCheckVal, i);
            ut2.Call(ThreadIncrement);
            ut1.Call(ThreadCheckVal, i + 1);
            ut2.Call(ThreadDecrement);
          }
          for (int i = maxVal1 - 1; i >= 0; i--)
          {
            ut1.Call(ThreadDecrement);
            ut2.Call(ThreadCheckVal, i);
            ut2.Call(ThreadIncrement);
            ut2.Call(ThreadCheckVal, i + 1);
            ut1.Call(ThreadDecrement);
          }
        }
      }
    }

    [Test]
    public void Concurrency()
    {
      Thread t1;
      Thread t2;
      UnitFnMethod incrDeleg = new UnitFnMethod(ThreadIncrement);
      UnitFnMethod decrDeleg = new UnitFnMethod(ThreadDecrement);
      m_val = 0;

      t1 = new Thread(ConcurrencyTestStart);
      t2 = new Thread(ConcurrencyTestStart);
      t1.Start(incrDeleg);
      t2.Start(incrDeleg);
      t1.Join();
      t2.Join();
      Assert.AreEqual(2*maxVal2, m_val);

      t1 = new Thread(ConcurrencyTestStart);
      t2 = new Thread(ConcurrencyTestStart);
      t1.Start(decrDeleg);
      t2.Start(decrDeleg);
      t1.Join();
      t2.Join();
      Assert.AreEqual(0, m_val);

      t1 = new Thread(ConcurrencyTestStart);
      t2 = new Thread(ConcurrencyTestStart);
      t1.Start(incrDeleg);
      t2.Start(decrDeleg);
      t1.Join();
      t2.Join();
      Assert.AreEqual(0, m_val);

      t1 = new Thread(ConcurrencyTestStart);
      t2 = new Thread(ConcurrencyTestStart);
      t1.Start(decrDeleg);
      t2.Start(incrDeleg);
      t1.Join();
      t2.Join();
      Assert.AreEqual(0, m_val);
    }

    [Test]
    public void ProcIncrDecr()
    {
      BBComm.KeyValueMap[testKey] = 0;
      using (UnitProcess client1 = new UnitProcess())
      {
        using (UnitProcess client2 = new UnitProcess())
        {
          for (int i = 1; i <= maxVal1; i++)
          {
            client1.Call(ProcessIncrement);
            client2.Call(ProcessCheckVal, i);
            client2.Call(ProcessIncrement);
            client1.Call(ProcessCheckVal, i + 1);
            client2.Call(ProcessDecrement);
          }
          for (int i = maxVal1 - 1; i >= 0; i--)
          {
            client1.Call(ProcessDecrement);
            client2.Call(ProcessCheckVal, i);
            client2.Call(ProcessIncrement);
            client2.Call(ProcessCheckVal, i + 1);
            client1.Call(ProcessDecrement);
          }
        }
      }
    }

    [Test]
    public void ThreadGroup()
    {
      int numThreads = 50;
      m_val = 0;
      using (ClientGroup cg1 =
        ClientGroup.Create(UnitThread.Create, numThreads))
      {
        using (UnitThread ut1 = new UnitThread())
        {
          for (int i = 1; i <= maxVal1; i++)
          {
            cg1.Call(ThreadIncrement);
            ut1.Call(ThreadCheckVal, i * numThreads);
            ut1.Call(ThreadIncrement);
            cg1.Call(ThreadCheckVal, i * numThreads + 1);
            ut1.Call(ThreadDecrement);
          }
          for (int i = maxVal1 - 1; i >= 0; i--)
          {
            cg1.Call(ThreadDecrement);
            ut1.Call(ThreadCheckVal, i * numThreads);
            ut1.Call(ThreadIncrement);
            cg1.Call(ThreadCheckVal, i * numThreads + 1);
            ut1.Call(ThreadDecrement);
          }
        }
      }
    }
  }
}
