//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  public class NoopAuthInit
    : IAuthInitialize
  {
    NoopAuthInit() { }
    
    #region IAuthInitialize Members
    
    public Properties<string, object> GetCredentials(Properties<string, string> props, string server)
    {
      Properties<string, object> credentials = Properties<string, object>.Create<string, object>();
      return credentials;
    }
    public void Close()
    {
    }

    #endregion
  }
}
