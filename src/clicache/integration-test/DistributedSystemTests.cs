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

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("unicast_only")]
  public class DistributedSystemTests : UnitTests
  {
    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [Test]
    public void Connect()
    {
      try
      {
        DistributedSystem.Disconnect();
        Assert.Fail("NotConnectedException should have occurred when "
          + "disconnecting without having connected.");
      }
      catch (NotConnectedException ex)
      {
        Util.Log("Got an expected exception in DistributedSystem.disconnect: "
          + ex);
      }
      try
      {
        CacheHelper.ConnectName("ConnTest");
      }
      finally
      {
        CacheHelper.Close();
      }
    }

    [Test]
    public void ConnectToNull()
    {
      Util.Log("Creating DistributedSytem with null name...");
      try
      {
        CacheHelper.ConnectName(null);
        CacheHelper.Close();
        Assert.Fail("IllegalArgumentException should have occurred when "
          + "connecting to a null DistributedSystem.");
      }
      catch (IllegalArgumentException ex)
      {
        Util.Log("Got an expected exception in DistributedSystem.connect: "
          + ex);
      }
    }

    [Test]
    public void Reconnect()
    {
      string[] memberTypes = { "PEER", "SERVER" };
      foreach (string memberType in memberTypes)
      {
        // Connect and disconnect 10 times

        for (int i = 0; i < 10; i++)
        {
          CacheHelper.InitConfig(memberType, null);

          try
          {
            Region region1 = CacheHelper.CreatePlainRegion("R1");
            Region region2 = CacheHelper.CreatePlainRegion("R2");
          }
          finally
          {
            CacheHelper.Close();
          }
        }
      }
    }

    [Test]
    public void Example()
    {
      CacheableString cVal;

      Region region = CacheHelper.CreateLRURegion("exampleRegion",
        1000, ScopeType.DistributedNoAck);
      try
      {
        // put some values into the cache.
        for (int i = 1; i <= 2000; i++)
        {
          region.Put("key-" + i, "value-" + i);
        }

        // do some gets... printing what we find in the cache.
        for (int i = 1; i <= 2000; i += 100)
        {
          cVal = region.Get("key-" + i) as CacheableString;
          if (cVal == null)
          {
            Util.Log("Didn't find key-{0} in the cache.", i);
          }
          else
          {
            Util.Log("Found key-{0} with value {1}.", i, cVal.Value);
          }
        }
      }
      finally
      {
        CacheHelper.Close();
      }
    }
  }
}
