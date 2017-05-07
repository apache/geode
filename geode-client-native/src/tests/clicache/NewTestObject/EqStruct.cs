//=========================================================================
// (c) Copyright 2002-2007, GemStone Systems, Inc. All Rights Reserved.
// 1260 NW Waterhouse Ave., Suite 200,  Beaverton, OR 97006
//=========================================================================


using System;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
    using GemStone.GemFire.Cache.Generic;
    public class EqStruct
      : TimeStampdObject
    {
        private int myIndex;
        private string state;
        private long timestamp;
        double executedPriceSum;
        private int cxlQty;
        private int isSyntheticOrder;
        private long availQty;
        private double positionQty;
        private int isRestricted;
        private string demandInd;
        private string side;
        private int orderQty;
        private double price;
        private string ordType;
        private double stopPx;
        private string senderCompID;
        private string tarCompID;
        private string tarSubID;
        private string handlInst;
        private string orderID;
        private string timeInForce;
        private string clOrdID;
        private string orderCapacity;
        private int cumQty;
        private string symbol;
        private string symbolSfx;
        private string execInst;
        private string oldClOrdID;
        private double pegDifference;
        private string discretionInst;
        private double discretionOffset;
        private string financeInd;
        private string securityID;
        private string targetCompID;
        private string targetSubID;
        private int isDoneForDay;
        private int revisionSeqNum;
        private int replaceQty;
        private long usedClientAvailability;
        private string clientAvailabilityKey;
        private int isIrregularSettlmnt;

        private string var1;
        private string var2;
        private string var3;
        private string var4;
        private string var5;
        private string var6;
        private string var7;
        private string var8;
        private string var9;

        public EqStruct()
        {
        }

        public EqStruct(Int32 index)
        {
             myIndex = index;  // index
             state = "1";
             DateTime startTime = DateTime.Now;
             timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
             executedPriceSum = 5.5;
             cxlQty = 10;
             isSyntheticOrder = 0;
             availQty = 100;
             positionQty = 10.0;
             isRestricted = 1;
             demandInd = "ASDSAD";
             side = "16";
             orderQty = 3000;
             price = 78.9;
             ordType = "D";
             stopPx = 22.3;
             senderCompID = "dsafdsf";
             tarCompID  = "dsafsadfsaf";
             tarSubID  = "rwetwj";
             handlInst  = "M N";
             orderID  = "sample";
             timeInForce  = "4";
             clOrdID  = "sample";
             orderCapacity  = "6";
             cumQty = 0 ;
             symbol = "MSFT";
             symbolSfx = "0" ;
             execInst = "A";
             oldClOrdID = "";
             pegDifference = 0.1  ;
             discretionInst = "G";
             discretionOffset = 300.0;
             financeInd = "dsagfdsa";
             securityID = "MSFT.O";
             targetCompID = "LBLB";
             targetSubID = "EQUITY";
             isDoneForDay = 1;
             revisionSeqNum = 140;
             replaceQty = 0;
             usedClientAvailability = 45;
             clientAvailabilityKey = "UUUU";
             isIrregularSettlmnt = 1;

             var1 = "abcdefghijklmnopqrstuvwxyz";
             var2 = "abcdefghijklmnopqrstuvwxyz";
             var3 = "abcdefghijklmnopqrstuvwxyz";
             var4 = "abcdefghijklmnopqrstuvwxyz";
             var5 = "abcdefghijklmnopqrstuvwxyz";
             var6 = "abcdefghijklmnopqrstuvwxyz";
             var7 = "abcdefghijklmnopqrstuvwxyz";
             var8 = "abcdefghijklmnopqrstuvwxyz";
             var9 = "abcdefghijklmnopqrstuvwxyz";
        }


        public override UInt32 ObjectSize
        {
            get
            {
                return 0;
            }
        }
        public override UInt32 ClassId
        {
            get
            {
                return 101;
            }
        }
        public static IGFSerializable CreateDeserializable()
        {
            return new PSTObject();
        }
        public override long GetTimestamp()
        {
            return timestamp;
        }
        public override void ResetTimestamp()
        {
            DateTime startTime = DateTime.Now;
            timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
        }
        public override string ToString()
        {
            string portStr = string.Format("EqStruct [myIndex={0} state={1}", myIndex, state);
            return portStr;
        }

        public override IGFSerializable FromData(DataInput input)
        { //Strings
	      state = input.ReadUTF();
	      demandInd =input.ReadUTF();
	      side=input.ReadUTF();
	      ordType=input.ReadUTF();
	      senderCompID=input.ReadUTF();
	      tarCompID = input.ReadUTF();
	      tarSubID =input.ReadUTF();
	      handlInst=input.ReadUTF();
	      orderID=input.ReadUTF();
	      timeInForce=input.ReadUTF();
	      clOrdID=input.ReadUTF();
	      orderCapacity=input.ReadUTF();
	      symbol=input.ReadUTF();
	      symbolSfx=input.ReadUTF();
	      execInst=input.ReadUTF();
	      oldClOrdID=input.ReadUTF();
	      discretionInst=input.ReadUTF();
	      financeInd=input.ReadUTF();
	      securityID=input.ReadUTF();
	      targetCompID=input.ReadUTF();
	      targetSubID=input.ReadUTF();
	      clientAvailabilityKey=input.ReadUTF();
	      var1=input.ReadUTF();
	      var2=input.ReadUTF();
	      var3=input.ReadUTF();
	      var4=input.ReadUTF();
	      var5=input.ReadUTF();
	      var6=input.ReadUTF();
	      var7=input.ReadUTF();
	      var8=input.ReadUTF();
	      var9=input.ReadUTF();

	      //ints
	      myIndex=input.ReadInt32();
	      cxlQty=input.ReadInt32();
	      isSyntheticOrder=input.ReadInt32();
	      isRestricted=input.ReadInt32();
	      orderQty=input.ReadInt32();
	      cumQty=input.ReadInt32();
	      isDoneForDay=input.ReadInt32();
	      revisionSeqNum=input.ReadInt32();
	      replaceQty=input.ReadInt32();
	      isIrregularSettlmnt=input.ReadInt32();

	      //longs
	      timestamp=input.ReadInt64();
	      availQty=input.ReadInt64();
	      usedClientAvailability=input.ReadInt64();

	      //doubles
	      executedPriceSum=input.ReadDouble();
	      positionQty=input.ReadDouble();
	      price=input.ReadDouble();
	      stopPx=input.ReadDouble();
	      pegDifference=input.ReadDouble();
	      discretionOffset=input.ReadDouble();
	      return this;
        }
        public override void ToData(DataOutput output)
        {
          output.WriteUTF( state );
	      output.WriteUTF( demandInd );
	      output.WriteUTF( side );
	      output.WriteUTF( ordType );
	      output.WriteUTF( senderCompID  );
	      output.WriteUTF( tarCompID );
	      output.WriteUTF( tarSubID );
	      output.WriteUTF( handlInst );
	      output.WriteUTF( orderID );
	      output.WriteUTF( timeInForce   );
	      output.WriteUTF( clOrdID );
	      output.WriteUTF( orderCapacity );
	      output.WriteUTF( symbol );
	      output.WriteUTF( symbolSfx  );
	      output.WriteUTF( execInst );
	      output.WriteUTF( oldClOrdID );
	      output.WriteUTF( discretionInst );
	      output.WriteUTF( financeInd );
	      output.WriteUTF( securityID );
	      output.WriteUTF( targetCompID );
	      output.WriteUTF( targetSubID  );
	      output.WriteUTF( clientAvailabilityKey );
	      output.WriteUTF( var1 );
	      output.WriteUTF( var2 );
	      output.WriteUTF( var3 );
	      output.WriteUTF( var4 );
	      output.WriteUTF( var5 );
	      output.WriteUTF( var6 );
	      output.WriteUTF( var7 );
	      output.WriteUTF( var8 );
	      output.WriteUTF( var9 );

	      //ints
	      output.WriteInt32( myIndex );
	      output.WriteInt32( cxlQty );
	      output.WriteInt32( isSyntheticOrder );
	      output.WriteInt32( isRestricted );
	      output.WriteInt32( orderQty );
	      output.WriteInt32( cumQty );
	      output.WriteInt32( isDoneForDay );
	      output.WriteInt32( revisionSeqNum );
	      output.WriteInt32( replaceQty );
	      output.WriteInt32( isIrregularSettlmnt );

	      //longs
	      output.WriteInt64(timestamp);
	      output.WriteInt64(availQty );
	      output.WriteInt64(usedClientAvailability );

	      //doubles
	      output.WriteDouble( executedPriceSum );
	      output.WriteDouble( positionQty );
	      output.WriteDouble( price );
	      output.WriteDouble( stopPx );
	      output.WriteDouble( pegDifference );
	      output.WriteDouble( discretionOffset );
        }
    }
}