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
  public class DistOpsTests : DistOpsSteps
  {
    #region Private statics/constants and members

    private static string[] AckRegionNames = { "DistRegionAck1", "DistRegionNoAck1" };
    private static string[] ILRegionNames = { "IL_DistRegionAck", "IL_DistRegionNoAck" };

    private UnitProcess m_client1, m_client2, m_client3;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
    }
 
    [Test]
    public void DistOps()
    {
      m_client1.Call(CreateRegions, AckRegionNames);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateRegions, AckRegionNames);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFive, true);
      Util.Log("StepFive complete.");

      Util.Log("StepSix commencing.");
      m_client2.Call(StepSix, true);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSeven);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEight);
      Util.Log("StepEight complete.");

      m_client1.Call(StepNine);
      Util.Log("StepNine complete.");

      m_client2.Call(StepTen);
      Util.Log("StepTen complete.");

      m_client1.Call(StepEleven);
      Util.Log("StepEleven complete.");
    }

  }
}
