//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;

namespace GemStone.GemFire.Cache.FwkBBServer
{
  using GemStone.GemFire.DUnitFramework;

  static class FwkBBServer
  {
    static int Main(string[] args)
    {
      if (args == null || args.Length != 1)
      {
        Console.WriteLine("Please provide the TCP port as the argument.");
        return 1;
      }
      try
      {
        // NOTE: This is required so that remote client receives custom exceptions
        RemotingConfiguration.CustomErrorsMode = CustomErrorsModes.Off;

        BinaryServerFormatterSinkProvider serverProvider =
          new BinaryServerFormatterSinkProvider();
        serverProvider.TypeFilterLevel = TypeFilterLevel.Full;
        BinaryClientFormatterSinkProvider clientProvider =
          new BinaryClientFormatterSinkProvider();
        Dictionary<string, string> properties = new Dictionary<string, string>();

        properties["port"] = args[0];
        TcpChannel channel = new TcpChannel(properties, clientProvider, serverProvider);
        ChannelServices.RegisterChannel(channel, false);

        RemotingConfiguration.RegisterWellKnownServiceType(typeof(BBComm),
          CommConstants.BBService, WellKnownObjectMode.SingleCall);
        Console.WriteLine("Started the BB server on port {0}.", args[0]);
      }
      catch (Exception ex)
      {
        Util.Log("FATAL: Exception caught: {0}", ex);
      }
      System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
      return 0;
    }
  }
}
