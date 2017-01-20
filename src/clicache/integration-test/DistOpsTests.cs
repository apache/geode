/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading;

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.DUnitFramework;

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
