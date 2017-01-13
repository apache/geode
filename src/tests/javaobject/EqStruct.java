/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package javaobject;

import java.io.*;

import org.apache.geode.DataSerializable;
//import org.apache.geode.Instantiator;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.cache.Declarable;
import org.apache.geode.*;
import java.util.Properties;
import java.lang.Exception;
/**
 * The EqStruct is based on the Lehman Benchmark and provides us with
 * a flat object graph with many fields that implements the DataSerializable
 * interface and avoids the overhead associated with reflection during 
 * seralization.
 *
 *
 */
public class EqStruct implements Declarable,Serializable,DataSerializable {

  private int myIndex;
  private String state;
  private long timestamp;
  private double executedPriceSum;
  private int cxlQty;
  private int isSyntheticOrder;
  private long availQty;
  private double positionQty;
  private int isRestricted;
  private String demandInd;
  private String side;
  private int orderQty;
  private double price;
  private String ordType;
  private double stopPx;
  private String senderCompID;
  private String tarCompID;
  private String tarSubID;
  private String handlInst;
  private String orderID;
  private String timeInForce;
  private String clOrdID;
  private String orderCapacity;
  private int  cumQty;
  private String symbol;
  private String  symbolSfx;
  private String  execInst;
  private String oldClOrdID;
  private double pegDifference;
  private String  discretionInst;
  private double discretionOffset;
  private String  financeInd;
  private String  securityID;
  private String  targetCompID;
  private String targetSubID;
  private int isDoneForDay;
  private int revisionSeqNum;
  private int replaceQty;
  private long usedClientAvailability;
  private String clientAvailabilityKey;
  private int isIrregularSettlmnt;

  private String var1;
  private String var2;
  private String var3;
  private String var4;
  private String var5;
  private String var6;
  private String var7;
  private String var8;
  private String var9;

  

  static {
	Instantiator.register(new Instantiator(EqStruct.class, (byte)101) {
	  public DataSerializable newInstance() {
	    return new EqStruct();
	  }
	});
   }
 
  public void init( Properties props ) {
	  this.cxlQty = Integer.parseInt(props.getProperty("cxlQty"));
 }
  public EqStruct() {
  }
 
  /**
   * Returns the index encoded in the give <code>byte</code> array.
   *
   * @throws ObjectAccessException
   *         The index cannot be decoded from <code>bytes</code>
   */
  public int getIndex( ) {
    return this.myIndex;
  }

  
  //--------------------------------------------------------------------------
  // Implementation of UpdatableObject interface
  //--------------------------------------------------------------------------

    public void update()
    {
       var1 = "abcdefghi";
       cumQty = 39;
       usedClientAvailability = 0x8000000000001234L;
       discretionOffset = 12.3456789;
       resetTimestamp();
    }

  //--------------------------------------------------------------------------
  // Implementation of DataSerializable interface
  //--------------------------------------------------------------------------


    // INSTANTIATORS DISABLED due to bug 35646
    //
    // Avoid overhead of reflection via the gemstone.gemfire.Instantiator class
    //static {
    //  Instantiator.register(new Instantiator(EqStruct.class, (byte) 5) {
    //    public DataSerializable newInstance() {
    //      return new EqStruct();
    //    }
    //  });
    //}

    public void toData(DataOutput out) throws IOException {

      //Strings
      out.writeUTF( state );
      out.writeUTF( demandInd );
      out.writeUTF( side );
      out.writeUTF( ordType );
      out.writeUTF( senderCompID  );
      out.writeUTF( tarCompID );
      out.writeUTF( tarSubID );
      out.writeUTF( handlInst );
      out.writeUTF( orderID );
      out.writeUTF( timeInForce   );
      out.writeUTF( clOrdID );
      out.writeUTF( orderCapacity );
      out.writeUTF( symbol );
      out.writeUTF( symbolSfx  );
      out.writeUTF( execInst );
      out.writeUTF( oldClOrdID );
      out.writeUTF( discretionInst );
      out.writeUTF( financeInd );
      out.writeUTF( securityID );
      out.writeUTF( targetCompID );
      out.writeUTF( targetSubID  );
      out.writeUTF( clientAvailabilityKey );
      out.writeUTF( var1 );
      out.writeUTF( var2 );
      out.writeUTF( var3 );
      out.writeUTF( var4 );
      out.writeUTF( var5 );
      out.writeUTF( var6 );
      out.writeUTF( var7 );
      out.writeUTF( var8 );
      out.writeUTF( var9 );

      //ints
      out.writeInt( myIndex );
      out.writeInt( cxlQty );
      out.writeInt( isSyntheticOrder );
      out.writeInt( isRestricted );
      out.writeInt( orderQty );
      out.writeInt( cumQty );
      out.writeInt( isDoneForDay );
      out.writeInt( revisionSeqNum );
      out.writeInt( replaceQty );
      out.writeInt( isIrregularSettlmnt );

      //longs
      out.writeLong( timestamp );
      out.writeLong( availQty );
      out.writeLong( usedClientAvailability );

      //doubles
      out.writeDouble( executedPriceSum );
      out.writeDouble( positionQty );
      out.writeDouble( price );
      out.writeDouble( stopPx );
      out.writeDouble( pegDifference );
      out.writeDouble( discretionOffset );

    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

      //Strings
      state = in.readUTF();
      demandInd = in.readUTF();
      side = in.readUTF();
      ordType = in.readUTF();
      senderCompID  = in.readUTF();
      tarCompID = in.readUTF();
      tarSubID = in.readUTF();
      handlInst = in.readUTF();
      orderID = in.readUTF();
      timeInForce   = in.readUTF();
      clOrdID = in.readUTF();
      orderCapacity = in.readUTF();
      symbol = in.readUTF();
      symbolSfx  = in.readUTF();
      execInst = in.readUTF();
      oldClOrdID = in.readUTF();
      discretionInst = in.readUTF();
      financeInd = in.readUTF();
      securityID = in.readUTF();
      targetCompID = in.readUTF();
      targetSubID  = in.readUTF();
      clientAvailabilityKey = in.readUTF();
      var1 = in.readUTF();
      var2 = in.readUTF();
      var3 = in.readUTF();
      var4 = in.readUTF();
      var5 = in.readUTF();
      var6 = in.readUTF();
      var7 = in.readUTF();
      var8 = in.readUTF();
      var9 = in.readUTF();

      //ints
      myIndex = in.readInt();
      cxlQty = in.readInt();
      isSyntheticOrder = in.readInt();
      isRestricted = in.readInt();
      orderQty = in.readInt();
      cumQty = in.readInt();
      isDoneForDay = in.readInt();
      revisionSeqNum = in.readInt();
      replaceQty = in.readInt();
      isIrregularSettlmnt = in.readInt();

      //longs
      timestamp = in.readLong();
      availQty = in.readLong();
      usedClientAvailability = in.readLong();

      //doubles
      executedPriceSum = in.readDouble();
      positionQty = in.readDouble();
      price = in.readDouble();
      stopPx = in.readDouble();
      pegDifference = in.readDouble();
      discretionOffset = in.readDouble();

    }

  //--------------------------------------------------------------------------
  // Implementation of TimestampedObject interface
  //--------------------------------------------------------------------------

  public long getTimestamp() {
    return this.timestamp;
  }

  public void resetTimestamp() {
    this.timestamp = NanoTimer.getTime();
  }
}
