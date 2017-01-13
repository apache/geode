using System;
using System.Collections.Generic;
namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  public class DeltaTestImpl
    : IGFSerializable, IGFDelta
  {
    private static sbyte INT_MASK = 0x1;
    private static sbyte STR_MASK = 0X2;
    private static sbyte DOUBLE_MASK = 0x4;
    private static sbyte BYTE_ARR_MASK = 0x8;
    private static sbyte TEST_OBJ_MASK = 0x10;
    private static Int64 toDataCount = 0;
    private static Int64 fromDataCount = 0;
    private static object LOCK_THIS_CLASS = new object();
    private int intVar = 0;
    private string str;
    private double doubleVar;
    private byte[] byteArr;
    private TestObject1 testobj;
    private sbyte deltaBits = 0x0;
    private bool hasDelta = false;
    private Int64 toDeltaCounter;
    private Int64 fromDeltaCounter;

    public DeltaTestImpl()
    {
      intVar = 1;
      str = "test";
      doubleVar = 1.1;
      byte [] arr2 = new byte[1];
      byteArr = arr2;
      testobj = null;
      hasDelta = false;
      deltaBits = 0;
      toDeltaCounter = 0;
      fromDeltaCounter = 0;
    }

    public DeltaTestImpl(DeltaTestImpl rhs)
    {
      this.intVar = rhs.intVar;
      this.str = rhs.str;
      this.doubleVar=rhs.doubleVar;
      this.byteArr = rhs.byteArr;
      this.testobj = rhs.testobj;
      this.toDeltaCounter = rhs.GetToDeltaCounter();
      this.fromDeltaCounter = rhs.GetFromDeltaCounter();
    }
    public DeltaTestImpl(Int32 intValue, string strValue)
    {
      this.intVar = intValue;
      this.str = strValue;
    }

    public DeltaTestImpl(Int32 intValue, string strValue, double doubleVal, byte[] bytes, TestObject1 testObject)
    {
      this.intVar = intValue;
      this.str = strValue;
      this.doubleVar = doubleVal;
      this.byteArr = bytes;
      this.testobj = testObject;
    }

    public UInt32 ObjectSize
    {
      get
      {
        return 0;
      }
    }
    public UInt32 ClassId
    {
      get
      {
        return 0x1E;
      }
    }
    public static IGFSerializable CreateDeserializable()
    {
      return new DeltaTestImpl();
    }

    public Int64 GetToDeltaCounter()
    {
      return toDeltaCounter;
    }
    public Int64 GetFromDeltaCounter()
    {
      return fromDeltaCounter;
    }

    public static Int64 GetToDataCount()
    {
      return toDataCount;
    }

    public static Int64 GetFromDataCount()
    {
      return fromDataCount;
    }

    public static void ResetDataCount()
    {
      lock (LOCK_THIS_CLASS)
      {
        toDataCount = 0;
        fromDataCount = 0;
      }
    }

    public void SetIntVar(Int32 value)
    {
      intVar = value;
      deltaBits |= INT_MASK;
      hasDelta = true;
    }
    public Int32 GetIntVar()
    {
      return intVar;
    }

    public void SetStr(string str1)
    {
      str = str1;
    }

    public string GetStr()
    {
      return str;
    }
    public void SetDoubleVar(double value)
    {
      doubleVar = value;
      deltaBits |= DOUBLE_MASK;
      hasDelta = true;
    }
    public double GetSetDoubleVar()
    {
      return doubleVar;
    }
    public void SetByteArr(byte[] value)
    {
      byteArr = value;
      deltaBits |= BYTE_ARR_MASK;
      hasDelta = true;
    }
    public byte[] GetByteArr()
    {
      return (byte[])byteArr;
    }
    public TestObject1 GetTestObj()
    {
      return testobj;
    }
    public void SetTestObj(TestObject1 testObj)
    {
      this.testobj = testObj;
      deltaBits |= TEST_OBJ_MASK;
      hasDelta = true;

    }
    public void SetDelta(bool value)
    {
      hasDelta = value;
    }
    public bool HasDelta()
    {
      return hasDelta;
    }

    public IGFSerializable FromData(DataInput input)
    {
      intVar = input.ReadInt32();
      str = (string)input.ReadObject();
      doubleVar = input.ReadDouble();
      byteArr = (byte[])input.ReadObject();
      testobj = (TestObject1)input.ReadObject();
      lock (LOCK_THIS_CLASS)
      {
      fromDataCount++;
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(intVar);
      output.WriteObject(str);
      output.WriteDouble(doubleVar);
      output.WriteObject(byteArr);
      output.WriteObject(testobj);
      lock (LOCK_THIS_CLASS)
      {
        toDataCount++;
      }
    }
    public void ToDelta(DataOutput output)
    {
      lock (LOCK_THIS_CLASS)
      {
        toDeltaCounter++;
      }
      output.WriteSByte(deltaBits);
      if (deltaBits != 0)
      {
        if ((deltaBits & INT_MASK) == INT_MASK)
        {
          output.WriteInt32(intVar);
        }
        if ((deltaBits & STR_MASK) == STR_MASK)
        {
          output.WriteObject(str);
        }
        if ((deltaBits & DOUBLE_MASK) == DOUBLE_MASK)
        {
          output.WriteDouble(doubleVar);
        }
        if ((deltaBits & BYTE_ARR_MASK) == BYTE_ARR_MASK)
        {
          output.WriteObject(byteArr);
         }
        if ((deltaBits & TEST_OBJ_MASK) == TEST_OBJ_MASK)
        {
          output.WriteObject(testobj);
        }
      }
    }

    public void FromDelta(DataInput input)
    {
      lock (LOCK_THIS_CLASS)
      {
        fromDeltaCounter++;
      }
      deltaBits = input.ReadSByte();
      if ((deltaBits & INT_MASK) == INT_MASK)
      {
        intVar = input.ReadInt32();
      }
      if ((deltaBits & STR_MASK) == STR_MASK)
      {
        str = (string)input.ReadObject();
      }
      if ((deltaBits & DOUBLE_MASK) == DOUBLE_MASK)
      {
        doubleVar = input.ReadDouble();
      }
      if ((deltaBits & BYTE_ARR_MASK) == BYTE_ARR_MASK)
      {
        byteArr = (byte[])input.ReadObject();
      }
      if ((deltaBits & TEST_OBJ_MASK) == TEST_OBJ_MASK)
      {
        testobj = (TestObject1)input.ReadObject();
      }
    }
    public override string ToString()
    {
      string portStr = string.Format("DeltaTestImpl [hasDelta={0} int={1} " +
        "double={2} str={3}]", hasDelta, intVar, doubleVar, str);
      return portStr;
    }
  }
}
