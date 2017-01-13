//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Reflection;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("unicast_only")]
  public class DataIOTests : UnitTests
  {
    XmlNodeReaderWriter settings = Util.DefaultSettings;

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [Test]
    public void Byte()
    {
      List<Dictionary<string, string>> testbytes = settings.GetValues(MethodBase.GetCurrentMethod(), "byte");
      if (testbytes != null)
      {
        foreach (Dictionary<string, string> dEntry in testbytes)
        {
          DataOutput dataOutput = new DataOutput();
          byte testbyte = Util.String2Byte(dEntry["value"]);
          dataOutput.WriteByte(testbyte);
          byte[] buffer = dataOutput.GetBuffer();
          Assert.AreEqual(testbyte, buffer[0]);

          DataInput dataInput = new DataInput(buffer);
          byte result = dataInput.ReadByte();
          Assert.AreEqual(testbyte, result);
        }
      }
    }

    [Test]
    public void Boolean()
    {
      List<Dictionary<string, string>> testbools = settings.GetValues(MethodBase.GetCurrentMethod(), "bool");
      if (testbools != null)
      {
        foreach (Dictionary<string, string> dEntry in testbools)
        {
          DataOutput dataOutput = new DataOutput();
          bool testbool = bool.Parse(dEntry["value"]);
          dataOutput.WriteBoolean(testbool);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          bool result = dataInput.ReadBoolean();
          Assert.AreEqual(testbool, result);
        }
      }
    }

    [Test]
    public void Int16()
    {
      List<Dictionary<string, string>> testshorts = settings.GetValues(MethodBase.GetCurrentMethod(), "short");
      if (testshorts != null)
      {
        foreach (Dictionary<string, string> dEntry in testshorts)
        {
          DataOutput dataOutput = new DataOutput();
          short testshort = Util.String2Int16(dEntry["value"]);
          dataOutput.WriteInt16(testshort);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          short result = dataInput.ReadInt16();
          Assert.AreEqual(testshort, result);
        }
      }
    }

    [Test]
    public void Int32()
    {
      List<Dictionary<string, string>> testints = settings.GetValues(MethodBase.GetCurrentMethod(), "int");
      if (testints != null)
      {
        foreach (Dictionary<string, string> dEntry in testints)
        {
          DataOutput dataOutput = new DataOutput();
          int testint = Util.String2Int32(dEntry["value"]);
          dataOutput.WriteInt32(testint);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          int result = dataInput.ReadInt32();
          Assert.AreEqual(testint, result);
        }
      }
    }

    [Test]
    public void Int64()
    {
      List<Dictionary<string, string>> testints = settings.GetValues(MethodBase.GetCurrentMethod(), "int");
      if (testints != null)
      {
        foreach (Dictionary<string, string> dEntry in testints)
        {
          DataOutput dataOutput = new DataOutput();
          long testlong = Util.String2Int64(dEntry["value"]);
          dataOutput.WriteInt64(testlong);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          long result = dataInput.ReadInt64();
          Assert.AreEqual(testlong, result);
        }
      }
    }

    [Test]
    public void Float()
    {
      List<Dictionary<string, string>> testfloats = settings.GetValues(MethodBase.GetCurrentMethod(), "float");
      if (testfloats != null)
      {
        foreach (Dictionary<string, string> dEntry in testfloats)
        {
          DataOutput dataOutput = new DataOutput();
          float testfloat = float.Parse(dEntry["value"]);
          dataOutput.WriteFloat(testfloat);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          float result = dataInput.ReadFloat();
          Assert.AreEqual(testfloat, result);
        }
      }
    }

    [Test]
    public void Double()
    {
      List<Dictionary<string, string>> testdoubles = settings.GetValues(MethodBase.GetCurrentMethod(), "double");
      if (testdoubles != null)
      {
        foreach (Dictionary<string, string> dEntry in testdoubles)
        {
          DataOutput dataOutput = new DataOutput();
          double testdouble = double.Parse(dEntry["value"]);
          dataOutput.WriteDouble(testdouble);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          double result = dataInput.ReadDouble();
          Assert.AreEqual(testdouble, result);
        }
      }
    }

    [Test]
    public void ASCIIString()
    {
      List<Dictionary<string, string>> testasciis = settings.GetValues(MethodBase.GetCurrentMethod(), "ascii");
      if (testasciis != null)
      {
        foreach (Dictionary<string, string> dEntry in testasciis)
        {
          DataOutput dataOutput = new DataOutput();
          string testascii = dEntry["value"];
          dataOutput.WriteUTF(testascii);
          byte[] buffer = dataOutput.GetBuffer();
          Assert.AreEqual(Util.String2Byte(dEntry["byte0"]), buffer[0]);
          Assert.AreEqual(Util.String2Byte(dEntry["byte1"]), buffer[1]);
          for (int i = 0; i < testascii.Length; i++)
          {
            Assert.AreEqual(testascii[i], buffer[i + 2]);
          }

          DataInput dataInput = new DataInput(buffer);
          string result = dataInput.ReadUTF();
          Assert.AreEqual(testascii.Length, result.Length);
          Assert.AreEqual(testascii, result);
        }
      }
    }

    [Test]
    public void UTFString()
    {
      List<Dictionary<string, string>> testutfs = settings.GetValues(MethodBase.GetCurrentMethod(), "utf");
      if (testutfs != null)
      {
        foreach (Dictionary<string, string> dEntry in testutfs)
        {
          DataOutput dataOutput = new DataOutput();
          string testutf = Util.String2String(dEntry["value"]);
          dataOutput.WriteUTF(testutf);
          byte[] buffer = dataOutput.GetBuffer();
          byte[] expectedBytes = Util.String2Bytes(dEntry["bytes"]);
          Util.CompareTestArrays(expectedBytes, buffer);

          DataInput dataInput = new DataInput(buffer);
          string result = dataInput.ReadUTF();
          Assert.AreEqual(testutf.Length, result.Length);
          Assert.AreEqual(testutf, result);
        }
      }
    }
  }
}
