//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;


  [TestFixture]
  [Category("generics")]
  public class SerializationTests : ThinClientRegionSteps
  {
    private const int OTHER_TYPE1 = 1;
    private const int OTHER_TYPE2 = 2;
    private const int OTHER_TYPE22 = 3;
    private const int OTHER_TYPE4 = 4;
    private const int OTHER_TYPE42 = 5;
    private const int OTHER_TYPE43 = 6;

    private UnitProcess sender, receiver;

    protected override ClientBase[] GetClients()
    {
      sender = new UnitProcess();
      receiver = new UnitProcess();
      return new ClientBase[] { sender, receiver };
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      CacheHelper.StopJavaServers();
      base.EndTests();
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        sender.Call(DestroyRegions);
        receiver.Call(DestroyRegions);
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    private IGFSerializable CreateOtherType(int i, int otherType)
    {
      IGFSerializable ot;
      switch (otherType)
      {
        case OTHER_TYPE1: ot = new OtherType(i, i + 20000); break;
        case OTHER_TYPE2: ot = new OtherType2(i, i + 20000); break;
        case OTHER_TYPE22: ot = new OtherType22(i, i + 20000); break;
        case OTHER_TYPE4: ot = new OtherType4(i, i + 20000); break;
        case OTHER_TYPE42: ot = new OtherType42(i, i + 20000); break;
        case OTHER_TYPE43: ot = new OtherType43(i, i + 20000); break;
        default: ot = new OtherType(i, i + 20000); break;
      }
      return ot;
    }

    #region Functions that are invoked by the tests

    public void CreateRegionForOT(string locators)
    {
      CacheHelper.CreateTCRegion2<object, object>(RegionNames[0], true, false,
        null, locators, false);
      Serializable.RegisterTypeGeneric(OtherType.CreateDeserializable);
      Serializable.RegisterTypeGeneric(OtherType2.CreateDeserializable);
      Serializable.RegisterTypeGeneric(OtherType22.CreateDeserializable);
      Serializable.RegisterTypeGeneric(OtherType4.CreateDeserializable);
      Serializable.RegisterTypeGeneric(OtherType42.CreateDeserializable);
      Serializable.RegisterTypeGeneric(OtherType43.CreateDeserializable);
    }

    public void DoNPuts(int n)
    {
      try
      {
        Serializable.RegisterTypeGeneric(OtherType.CreateDeserializable);
        Assert.Fail("Expected exception in registering the type again.");
      }
      catch (IllegalStateException ex)
      {
        Util.Log("Got expected exception in RegisterType: {0}", ex);
      }
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      for (int i = 0; i < n; i++)
      {
        //CacheableInt32 key = new CacheableInt32(i);
        //region.Put(key, key);

        int key = i;
        region[key] = key;
      }
    }

    public void DoValidates(int n)
    {
      try
      {
        Serializable.RegisterTypeGeneric(OtherType.CreateDeserializable);
        Assert.Fail("Expected exception in registering the type again.");
      }
      catch (IllegalStateException ex)
      {
        Util.Log("Got expected exception in RegisterType: {0}", ex);
      }
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      for (int i = 0; i < n; i++)
      {
        //CacheableInt32 val = region.Get(i) as CacheableInt32;
        object val = region[i];
        Assert.AreEqual(i, val, "Found unexpected value");
      }
    }

    public void DoNPutsOtherType(int n, int otherType)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      for (int i = 0; i < n; i++)
      {
        IGFSerializable ot = CreateOtherType(i, otherType);
        region[i + 10] = ot;
      }
    }

    public void DoValidateNPutsOtherType(int n, int otherType)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      for (int i = 0; i < n; i++)
      {
        object val = region[i + 10];
        IGFSerializable ot = CreateOtherType(i, otherType);
        Assert.IsTrue(ot.Equals(val), "Found unexpected value");
      }
    }

    #endregion

    #region Tests

    [Test]
    public void CustomTypes()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      sender.Call(CreateRegionForOT, CacheHelper.Locators);
      Util.Log("StepOne complete.");

      receiver.Call(CreateRegionForOT, CacheHelper.Locators);
      Util.Log("StepTwo complete.");

      sender.Call(DoNPuts, 10);
      receiver.Call(DoValidates, 10);
      Util.Log("StepThree complete.");

      sender.Call(DoNPutsOtherType, 10, OTHER_TYPE1);
      receiver.Call(DoValidateNPutsOtherType, 10, OTHER_TYPE1);
      Util.Log("StepFour complete.");

      sender.Call(DoNPutsOtherType, 10, OTHER_TYPE2);
      receiver.Call(DoValidateNPutsOtherType, 10, OTHER_TYPE2);
      Util.Log("StepFive complete.");

      sender.Call(DoNPutsOtherType, 10, OTHER_TYPE22);
      receiver.Call(DoValidateNPutsOtherType, 10, OTHER_TYPE22);
      Util.Log("StepSix complete.");

      sender.Call(DoNPutsOtherType, 10, OTHER_TYPE4);
      receiver.Call(DoValidateNPutsOtherType, 10, OTHER_TYPE4);
      Util.Log("StepSeven complete.");

      sender.Call(DoNPutsOtherType, 10, OTHER_TYPE42);
      receiver.Call(DoValidateNPutsOtherType, 10, OTHER_TYPE42);
      Util.Log("StepEight complete.");

      sender.Call(DoNPutsOtherType, 10, OTHER_TYPE43);
      receiver.Call(DoValidateNPutsOtherType, 10, OTHER_TYPE43);
      Util.Log("StepNine complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
    }

    #endregion
  }

  [Serializable]
  public struct CData
  {
    #region Private members

    private Int32 m_first;
    private Int64 m_second;

    #endregion

    #region Public accessors

    public Int32 First
    {
      get
      {
        return m_first;
      }
      set
      {
        m_first = value;
      }
    }

    public Int64 Second
    {
      get
      {
        return m_second;
      }
      set
      {
        m_second = value;
      }
    }

    #endregion

    public CData(Int32 first, Int64 second)
    {
      m_first = first;
      m_second = second;
    }

    public static bool operator ==(CData obj1, CData obj2)
    {
      return ((obj1.m_first == obj2.m_first) && (obj1.m_second == obj2.m_second));
    }

    public static bool operator !=(CData obj1, CData obj2)
    {
      return ((obj1.m_first != obj2.m_first) || (obj1.m_second != obj2.m_second));
    }

    public override bool Equals(object obj)
    {
      if (obj is CData)
      {
        CData otherObj = (CData)obj;
        return ((m_first == otherObj.m_first) && (m_second == otherObj.m_second));
      }
      return false;
    }

    public override int GetHashCode()
    {
      return m_first.GetHashCode() ^ m_second.GetHashCode();
    }
  };

  public class PdxCData : IPdxSerializable
  {
    #region Private members

    private Int32 m_first;
    private Int64 m_second;

    #endregion

    #region Public accessors

    public Int32 First
    {
      get
      {
        return m_first;
      }
      set
      {
        m_first = value;
      }
    }

    public Int64 Second
    {
      get
      {
        return m_second;
      }
      set
      {
        m_second = value;
      }
    }

    #endregion

    public PdxCData(Int32 first, Int64 second)
    {
      m_first = first;
      m_second = second;
    }

    public PdxCData() { }

    public static PdxCData CreateDeserializable()
    {
      return new PdxCData();
    }
    public static bool operator ==(PdxCData obj1, PdxCData obj2)
    {
      return ((obj1.m_first == obj2.m_first) && (obj1.m_second == obj2.m_second));
    }

    public static bool operator !=(PdxCData obj1, PdxCData obj2)
    {
      return ((obj1.m_first != obj2.m_first) || (obj1.m_second != obj2.m_second));
    }

    public override bool Equals(object obj)
    {
      if (obj is PdxCData)
      {
        PdxCData otherObj = (PdxCData)obj;
        return ((m_first == otherObj.m_first) && (m_second == otherObj.m_second));
      }
      return false;
    }

    public override int GetHashCode()
    {
      return m_first.GetHashCode() ;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_first = reader.ReadInt("m_first");
      m_second = reader.ReadLong("m_second");
    }

    public void ToData(IPdxWriter writer)
    {      
      writer.WriteInt("m_first", m_first);
      writer.MarkIdentityField("m_first");
      writer.WriteLong("m_second", m_second);
    }

    #endregion

   

    
  };

  public class OtherType : IGFSerializable
  {
    private CData m_struct;
    private ExceptionType m_exType;

    public enum ExceptionType
    {
      None,
      Gemfire,
      System,
      // below are with inner exceptions
      GemfireGemfire,
      GemfireSystem,
      SystemGemfire,
      SystemSystem
    }

    public OtherType()
    {
      m_exType = ExceptionType.None;
    }

    public OtherType(Int32 first, Int64 second)
      : this(first, second, ExceptionType.None)
    {
    }

    public OtherType(Int32 first, Int64 second, ExceptionType exType)
    {
      m_struct.First = first;
      m_struct.Second = second;
      m_exType = exType;
    }

    public CData Data
    {
      get
      {
        return m_struct;
      }
    }

    public static IGFSerializable Duplicate(IGFSerializable orig)
    {
      DataOutput dout = new DataOutput();
      orig.ToData(dout);

      DataInput din = new DataInput(dout.GetBuffer());
      IGFSerializable dup = (IGFSerializable)din.ReadObject();
      return dup;
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_struct.First = input.ReadInt32();
      m_struct.Second = input.ReadInt64();
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_struct.First);
      output.WriteInt64(m_struct.Second);
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int64));
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x0;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new OtherType();
    }

    public override int GetHashCode()
    {
      return m_struct.First.GetHashCode() ^ m_struct.Second.GetHashCode();
    }
    
    public override bool Equals(object obj)
    {
      OtherType ot = obj as OtherType;
      if (ot != null)
      {
        return (m_struct.Equals(ot.m_struct));
      }
      return false;
    }
  }

  public class OtherType2 : IGFSerializable
  {
    private CData m_struct;
    private ExceptionType m_exType;

    public enum ExceptionType
    {
      None,
      Gemfire,
      System,
      // below are with inner exceptions
      GemfireGemfire,
      GemfireSystem,
      SystemGemfire,
      SystemSystem
    }
    
    public OtherType2()
    {
      m_exType = ExceptionType.None;
    }

    public OtherType2(Int32 first, Int64 second)
      : this(first, second, ExceptionType.None)
    {
    }

    public OtherType2(Int32 first, Int64 second, ExceptionType exType)
    {
      m_struct.First = first;
      m_struct.Second = second;
      m_exType = exType;
    }

    public CData Data
    {
      get
      {
        return m_struct;
      }
    }

    public static IGFSerializable Duplicate(IGFSerializable orig)
    {
      DataOutput dout = new DataOutput();
      orig.ToData(dout);

      DataInput din = new DataInput(dout.GetBuffer());
      IGFSerializable dup = (IGFSerializable)din.ReadObject();
      return dup;
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_struct.First = input.ReadInt32();
      m_struct.Second = input.ReadInt64();
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_struct.First);
      output.WriteInt64(m_struct.Second);
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int64));
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x8C;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new OtherType2();
    }

    public override int GetHashCode()
    {
      return m_struct.First.GetHashCode() ^ m_struct.Second.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      OtherType2 ot = obj as OtherType2;
      if (ot != null)
      {
        return (m_struct.Equals(ot.m_struct));
      }
      return false;
    }

  }

  public class OtherType22 : IGFSerializable
  {
    private CData m_struct;
    private ExceptionType m_exType;

    public enum ExceptionType
    {
      None,
      Gemfire,
      System,
      // below are with inner exceptions
      GemfireGemfire,
      GemfireSystem,
      SystemGemfire,
      SystemSystem
    }

    public OtherType22()
    {
      m_exType = ExceptionType.None;
    }

    public OtherType22(Int32 first, Int64 second)
      : this(first, second, ExceptionType.None)
    {
    }

    public OtherType22(Int32 first, Int64 second, ExceptionType exType)
    {
      m_struct.First = first;
      m_struct.Second = second;
      m_exType = exType;
    }

    public CData Data
    {
      get
      {
        return m_struct;
      }
    }

    public static IGFSerializable Duplicate(IGFSerializable orig)
    {
      DataOutput dout = new DataOutput();
      orig.ToData(dout);

      DataInput din = new DataInput(dout.GetBuffer());
      IGFSerializable dup = (IGFSerializable)din.ReadObject();
      return dup;
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_struct.First = input.ReadInt32();
      m_struct.Second = input.ReadInt64();
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_struct.First);
      output.WriteInt64(m_struct.Second);
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int64));
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x8C0;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new OtherType22();
    }

    public override int GetHashCode()
    {
      return m_struct.First.GetHashCode() ^ m_struct.Second.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      OtherType22 ot = obj as OtherType22;
      if (ot != null)
      {
        return (m_struct.Equals(ot.m_struct));
      }
      return false;
    }
  }

  public class OtherType4 : IGFSerializable
  {
    private CData m_struct;
    private ExceptionType m_exType;

    public enum ExceptionType
    {
      None,
      Gemfire,
      System,
      // below are with inner exceptions
      GemfireGemfire,
      GemfireSystem,
      SystemGemfire,
      SystemSystem
    }

    public OtherType4()
    {
      m_exType = ExceptionType.None;
    }

    public OtherType4(Int32 first, Int64 second)
      : this(first, second, ExceptionType.None)
    {
    }

    public OtherType4(Int32 first, Int64 second, ExceptionType exType)
    {
      m_struct.First = first;
      m_struct.Second = second;
      m_exType = exType;
    }

    public CData Data
    {
      get
      {
        return m_struct;
      }
    }

    public static IGFSerializable Duplicate(IGFSerializable orig)
    {
      DataOutput dout = new DataOutput();
      orig.ToData(dout);

      DataInput din = new DataInput(dout.GetBuffer());
      IGFSerializable dup = (IGFSerializable)din.ReadObject();
      return dup;
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_struct.First = input.ReadInt32();
      m_struct.Second = input.ReadInt64();
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_struct.First);
      output.WriteInt64(m_struct.Second);
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int64));
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x8FC0;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new OtherType4();
    }

    public override int GetHashCode()
    {
      return m_struct.First.GetHashCode() ^ m_struct.Second.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      OtherType4 ot = obj as OtherType4;
      if (ot != null)
      {
        return (m_struct.Equals(ot.m_struct));
      }
      return false;
    }

  }

  public class OtherType42 : IGFSerializable
  {
    private CData m_struct;
    private ExceptionType m_exType;

    public enum ExceptionType
    {
      None,
      Gemfire,
      System,
      // below are with inner exceptions
      GemfireGemfire,
      GemfireSystem,
      SystemGemfire,
      SystemSystem
    }

    public OtherType42()
    {
      m_exType = ExceptionType.None;
    }

    public OtherType42(Int32 first, Int64 second)
      : this(first, second, ExceptionType.None)
    {
    }

    public OtherType42(Int32 first, Int64 second, ExceptionType exType)
    {
      m_struct.First = first;
      m_struct.Second = second;
      m_exType = exType;
    }

    public CData Data
    {
      get
      {
        return m_struct;
      }
    }

    public static IGFSerializable Duplicate(IGFSerializable orig)
    {
      DataOutput dout = new DataOutput();
      orig.ToData(dout);

      DataInput din = new DataInput(dout.GetBuffer());
      IGFSerializable dup = (IGFSerializable)din.ReadObject();
      return dup;
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_struct.First = input.ReadInt32();
      m_struct.Second = input.ReadInt64();
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_struct.First);
      output.WriteInt64(m_struct.Second);
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int64));
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x6F3F97;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new OtherType42();
    }

    public override int GetHashCode()
    {
      return m_struct.First.GetHashCode() ^ m_struct.Second.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      OtherType42 ot = obj as OtherType42;
      if (ot != null)
      {
        return (m_struct.Equals(ot.m_struct));
      }
      return false;
    }

  }

  public class OtherType43 : IGFSerializable
  {
    private CData m_struct;
    private ExceptionType m_exType;

    public enum ExceptionType
    {
      None,
      Gemfire,
      System,
      // below are with inner exceptions
      GemfireGemfire,
      GemfireSystem,
      SystemGemfire,
      SystemSystem
    }

    public OtherType43()
    {
      m_exType = ExceptionType.None;
    }

    public OtherType43(Int32 first, Int64 second)
      : this(first, second, ExceptionType.None)
    {
    }

    public OtherType43(Int32 first, Int64 second, ExceptionType exType)
    {
      m_struct.First = first;
      m_struct.Second = second;
      m_exType = exType;
    }

    public CData Data
    {
      get
      {
        return m_struct;
      }
    }

    public static IGFSerializable Duplicate(IGFSerializable orig)
    {
      DataOutput dout = new DataOutput();
      orig.ToData(dout);

      DataInput din = new DataInput(dout.GetBuffer());
      IGFSerializable dup = (IGFSerializable)din.ReadObject();
      return dup;
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_struct.First = input.ReadInt32();
      m_struct.Second = input.ReadInt64();
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_struct.First);
      output.WriteInt64(m_struct.Second);
      switch (m_exType)
      {
        case ExceptionType.None:
          break;
        case ExceptionType.Gemfire:
          throw new GemFireIOException("Throwing an exception");
        case ExceptionType.System:
          throw new IOException("Throwing an exception");
        case ExceptionType.GemfireGemfire:
          throw new GemFireIOException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.GemfireSystem:
          throw new CacheServerException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
        case ExceptionType.SystemGemfire:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new CacheServerException("This is an inner exception"));
        case ExceptionType.SystemSystem:
          throw new ApplicationException("Throwing an exception with inner " +
            "exception", new IOException("This is an inner exception"));
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int64));
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x7FFFFFFF;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new OtherType43();
    }

    public override int GetHashCode()
    {
      return m_struct.First.GetHashCode() ^ m_struct.Second.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      OtherType43 ot = obj as OtherType43;
      if (ot != null)
      {
        return (m_struct.Equals(ot.m_struct));
      }
      return false;
    }

  }
}
