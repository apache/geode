//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.DUnitFramework
{
  public static class ServerConnection<TComm>
  {
    public static TComm Connect(string serverUrl)
    {
      if (serverUrl == null || serverUrl.Length == 0)
      {
        throw new IllegalArgException("ServerConnection::ctor: " +
          "The serverUrl cannot be null or empty!!");
      }
      TComm serverComm = (TComm)Activator.GetObject(typeof(TComm), serverUrl);
      if (serverComm == null)
      {
        throw new ServerNotFoundException("Server at URL '" + serverUrl +
          "' not found.");
      }
      return serverComm;
    }
  }
}
