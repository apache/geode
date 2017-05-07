//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class BasicTests : UnitTests
  {
    #region constants

    const int WarmupIters = 1000000;
    const int PerfIters = 50000000;
    const int PerfObjectSize = 10;

    #endregion

    #region overridden members

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    public override void EndTest()
    {
      GC.Collect();
      GC.WaitForPendingFinalizers();
      base.EndTest();
    }

    #endregion

    #region P/Invoke calls for performance counters

    [DllImport("kernel32.dll")]
    private static extern bool QueryPerformanceCounter(
      out long lpPerformanceCount);

    [DllImport("kernel32.dll")]
    private static extern bool QueryPerformanceFrequency(
      out long lpFrequency);

    #endregion

    #region Tests

    [Test]
    public void UnsafeCSWrapperTest()
    {
      ManagedWrapper wrapper = new ManagedWrapper(100000000);
      Assert.IsFalse(wrapper.UnsafeDoOp(20000000, 5),
        "Expected the native type to be destroyed during UnsafeDoOp");
    }

    [Test]
    public void SafeCSWrapper1Test()
    {
      ManagedWrapper wrapper = new ManagedWrapper(100000000);
      Assert.IsTrue(wrapper.SafeDoOp1(20000000, 5),
        "Expected the native type not to be destroyed during SafeDoOp1");
    }

    [Test]
    public void SafeCSWrapper2Test()
    {
      ManagedWrapper wrapper = new ManagedWrapper(100000000);
      Assert.IsTrue(wrapper.SafeDoOp2(20000000, 5),
        "Expected the native type not to be destroyed during SafeDoOp2");
    }

    [Test]
    public void SafeCSWrapper3Test()
    {
      ManagedWrapper wrapper = new ManagedWrapper(100000000);
      Assert.IsTrue(wrapper.SafeDoOp3(20000000, 5),
        "Expected the native type not to be destroyed during SafeDoOp3");
      GC.KeepAlive(wrapper);
    }

    [Test]
    public void SafeCSWrapper1PerfTest()
    {
      long freq, start, end;
      ManagedWrapper wrapper = new ManagedWrapper(100000000);

      // warmup task
      bool res = true;
      for (int i = 1; i <= WarmupIters; ++i)
      {
        res &= wrapper.SafeDoOp1(PerfObjectSize, 0);
      }
      Assert.IsTrue(res,
        "Expected the native type not to be destroyed during SafeDoOp1");

      // timed task
      QueryPerformanceFrequency(out freq);
      QueryPerformanceCounter(out start);
      for (int i = 1; i <= PerfIters; ++i)
      {
        res &= wrapper.SafeDoOp1(PerfObjectSize, 0);
      }
      QueryPerformanceCounter(out end);
      double time = (double)(end - start) / (double)freq;

      Util.Log("Perf counter in wrapper1 test (managed reference wrapped " +
        "inside native temporary object) with result {0} is: {1}",
        res, time);
    }

    [Test]
    public void SafeCSWrapper2PerfTest()
    {
      long freq, start, end;
      ManagedWrapper wrapper = new ManagedWrapper(100000000);

      // warmup task
      bool res = true;
      for (int i = 1; i <= WarmupIters; ++i)
      {
        res &= wrapper.SafeDoOp2(PerfObjectSize, 0);
      }
      Assert.IsTrue(res,
        "Expected the native type not to be destroyed during SafeDoOp2");

      // timed task
      QueryPerformanceFrequency(out freq);
      QueryPerformanceCounter(out start);
      for (int i = 1; i <= PerfIters; ++i)
      {
        res &= wrapper.SafeDoOp2(PerfObjectSize, 0);
      }
      QueryPerformanceCounter(out end);
      double time = (double)(end - start) / (double)freq;

      Util.Log("Perf counter in wrapper2 test (using explicit GC.KeepAlive " +
        "call) with result {0} is: {1}", res, time);
    }

    [Test]
    public void SafeCSWrapper3PerfTest()
    {
      long freq, start, end;
      ManagedWrapper wrapper = new ManagedWrapper(100000000);

      // warmup task
      bool res = true;
      for (int i = 1; i <= WarmupIters; ++i)
      {
        res &= wrapper.SafeDoOp3(PerfObjectSize, 0);
      }
      Assert.IsTrue(res,
        "Expected the native type not to be destroyed during SafeDoOp3");

      // timed task
      QueryPerformanceFrequency(out freq);
      QueryPerformanceCounter(out start);
      for (int i = 1; i <= PerfIters; ++i)
      {
        res &= wrapper.SafeDoOp3(PerfObjectSize, 0);
      }
      QueryPerformanceCounter(out end);
      double time = (double)(end - start) / (double)freq;

      Util.Log("Perf counter in wrapper3 test (managed object wrapped " +
        "inside managed value object) with result {0} is: {1}", res, time);
    }

    #endregion
  }
}
