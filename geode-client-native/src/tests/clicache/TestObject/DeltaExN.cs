
using System;
namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  public class DeltaEx : IGFDelta,IGFSerializable, ICloneable
    {
        private int counter;
        private bool IsDelta;
        public static int ToDeltaCount = 0;
        public static int FromDeltaCount = 0;
        public static int ToDataCount = 0;
        public static int FromDataCount = 0;
        public static int CloneCount = 0;

        public DeltaEx()
        {
            counter = 24;
            IsDelta = false;
        }
        public DeltaEx(int count)
        {
            count = 0;
            IsDelta = false;
        }
        public bool HasDelta()
        {
            return IsDelta;
        }
        public void ToDelta(DataOutput DataOut)
        {
            DataOut.WriteInt32(counter);
            ToDeltaCount++;
        }

        public void FromDelta(DataInput DataIn)
        {
            int val = DataIn.ReadInt32();
            if( FromDeltaCount == 1 )
            {
                FromDeltaCount++;
                throw new InvalidDeltaException();
            }
            counter+=val;
            FromDeltaCount++;
        }

        public void ToData(DataOutput DataOut)
        {
            DataOut.WriteInt32( counter );
            ToDataCount++;
        }
        public IGFSerializable FromData(DataInput DataIn)
        {
            counter = DataIn.ReadInt32();
            FromDataCount++;
            return this;
        }

        public UInt32 ClassId
        {
            get
            {
                //UInt32 classId = 1;
              return 0x01;
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
        public void SetDelta(bool isDelta)
        {
            IsDelta = isDelta;
        }

        public static IGFSerializable create()
        {
            return new DeltaEx();
        }
        public Object Clone()
        {
          CloneCount++;
          return new DeltaEx();
        }
      public int getCounter()
      {
        return counter;
      }
    }
}
