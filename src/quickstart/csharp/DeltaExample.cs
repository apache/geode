
using System;
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  public class DeltaExample : IGFDelta,IGFSerializable, ICloneable
    {
      // data members
      private Int32 m_field1;
      private Int32 m_field2;
      private Int32 m_field3;

      // delta indicators
      private bool m_f1set;
      private bool m_f2set;
      private bool m_f3set;

      public DeltaExample(Int32 field1, Int32 field2, Int32 field3)
      {
        m_field1 = field1;
        m_field2 = field2;
        m_field3 = field3;
        reset();
      }

      public DeltaExample()
      {
        reset();
      }
      
      public DeltaExample(DeltaExample copy)
      {
        m_field1 = copy.m_field1;
        m_field2 = copy.m_field2;
        m_field3 = copy.m_field3;
        reset();
      }
  
      private void reset()
      {
        m_f1set = false;
        m_f2set = false;
        m_f3set = false;
      }
      
      public Int32 getField1()
      {
        return m_field1;
      }
  
      public Int32 getField2()
      {
        return m_field2;
      }

      public Int32 getField3()
      {
        return m_field3;
      }

      public void setField1(Int32 val)
      {
        lock(this)
        {
          m_field1 = val;
          m_f1set = true;
        }
      }

      public void setField2(Int32 val)
      {
        lock(this)
        {
          m_field2 = val;
          m_f2set = true;
        }
      }

      public void setField3(Int32 val)
      {
        lock(this)
        {
          m_field3 = val;
          m_f3set = true;
        }
      }
      
      public bool HasDelta()
      {
        return m_f1set || m_f2set || m_f3set;
      }

      public void ToDelta(DataOutput DataOut)
      {
        lock(this)
        {
          DataOut.WriteBoolean(m_f1set);
          if (m_f1set)
          {
            DataOut.WriteInt32(m_field1);
          }
          DataOut.WriteBoolean(m_f2set);
          if (m_f2set)
          {
            DataOut.WriteInt32(m_field2);
          }
          DataOut.WriteBoolean(m_f3set);
          if (m_f3set)
          {
            DataOut.WriteInt32(m_field3);
          }
          reset();
        }
      }

      public void FromDelta(DataInput DataIn)
      {
        lock(this)
        {
          m_f1set = DataIn.ReadBoolean();
          if (m_f1set)
          {
            m_field1 = DataIn.ReadInt32();
          }
          m_f2set = DataIn.ReadBoolean();
          if (m_f2set)
          {
            m_field2 = DataIn.ReadInt32();
          }
          m_f3set = DataIn.ReadBoolean();
          if (m_f3set)
          {
            m_field3 = DataIn.ReadInt32();
          }
          reset();
        }
      }

      public void ToData(DataOutput DataOut)
      {
        DataOut.WriteInt32(m_field1);
        DataOut.WriteInt32(m_field2);
        DataOut.WriteInt32(m_field3);
      }

      public IGFSerializable FromData(DataInput DataIn)
      {
        m_field1 = DataIn.ReadInt32();
        m_field2 = DataIn.ReadInt32();
        m_field3 = DataIn.ReadInt32();
        return this;
      }

      public UInt32 ClassId
      {
        get
        {
          return 0x02;
        }
      }

      public UInt32 ObjectSize
      {
        get
        {
          UInt32 objectSize = 0;
          return objectSize;
        }
      }
   
      public static IGFSerializable create()
      {
        return new DeltaExample();
      }

      public Object Clone()
      {
        return new DeltaExample(this);
      }

    }
}
