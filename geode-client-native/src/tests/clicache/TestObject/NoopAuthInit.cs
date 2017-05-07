//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests
{
  public class NoopAuthInit
    : IAuthInitialize
  {
    NoopAuthInit() { }
    public Properties GetCredentials(Properties props, string server)
    {
      Properties credentials = Properties.Create();
      return credentials;
    }
    public void Close()
    {
    }
  }
}
