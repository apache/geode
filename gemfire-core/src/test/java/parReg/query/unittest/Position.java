/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package parReg.query.unittest;

import java.util.*;
import java.io.Serializable;
import com.gemstone.gemfire.cache.Declarable;

/**
 * Represents a number of shares of a stock ("security") held in a
 * {@link NewPortfolio}.
 *
 * <P>
 *
 * This class is <code>Serializable</code> because we want it to be
 * distributed to multiple members of a distributed system.  Because
 * this class is <code>Declarable</code>, we can describe instances of
 * it in a GemFire <code>cache.xml</code> file.
 *
 * @author GemStone Systems, Inc.
 * @since 4.0
 */
public class Position implements Declarable, Serializable, Comparable {

  private static final Random rng = new Random();

  protected String secId;
  protected double qty;
  protected double mktValue;
  private final int NUM_OF_SECURITIES = 200;
  private final int MAX_PRICE = 100;
  
  public void init(Properties props) {
    this.secId = props.getProperty("secId");
    this.qty = Double.parseDouble(props.getProperty("qty"));
    this.mktValue = Double.parseDouble(props.getProperty("mktValue"));
  }
  
  /**
   * Returns the id of the security held in this position.
   */
  public String getSecId(){
    return this.secId;
  }
  
  /**
   * Returns the number of shares held in this position.
   */
  public double getQty(){
    return this.qty;
  }
    
  /**
   * Returns the value of this position.
   */
  public double getMktValue() {
    return this.mktValue;
  }

  public String toString(){
    return "Position [secId="+secId+" qty="+this.qty+" mktValue="+mktValue+"]";
  }
  
  public static String toString(List aList) {
    StringBuffer aStr = new StringBuffer();
    aStr.append("List of size " + aList.size() + "\n");
    for (int i = 0; i < aList.size(); i++) {
       Object anObj = aList.get(i);
       if (anObj instanceof Position) {
          Position p = (Position)(anObj);
          aStr.append(p.toString());
       }
       aStr.append("\n");
    }
    return aStr.toString();
 }

  /**
   * To enable the comparison.
   */
  public boolean equals(Object anObj) {
    if (anObj == null) {
       return false;
    }
    if (anObj.getClass() == this.getClass()) {
       Position pos = (Position)anObj;
       if ((pos.mktValue != this.mktValue) || (pos.qty != this.qty))  {
          return false;
       }

       if (pos.secId == null) {
          if (this.secId != null) {

             return false;
          }
       } else {
         if (!(pos.secId.equals(this.secId))) {

            return false;
         }
       }
    } else {

       return false;
    }
    return true;
 }
  
  public int hashCode() {
    int result = 17;
    result = 37 * result + (int) (Double.doubleToLongBits(mktValue)^(Double.doubleToLongBits(mktValue)>>>32));
    result = 37 * result + (int) (Double.doubleToLongBits(qty)^(Double.doubleToLongBits(qty)>>>32));
    result = 37 * result + secId.hashCode();
     
    return result;
  }
  
  /**
   * to configure position using index, set quantity equal to the index
   */
  public void init(int i) {
    this.secId = new Integer(rng.nextInt(NUM_OF_SECURITIES)).toString();
    this.qty = new Double(i).doubleValue();
    this.mktValue = new Double(rng.nextDouble() * MAX_PRICE ).doubleValue();
  }
  
  public int getIndex() {
    return (int)this.qty;
  }
  
  public void validate (int index){
  }

  @Override
  public int compareTo(Object o) {
    if( o == this) {
      return 0;
    }else {
      if (o instanceof Position) {
        return Integer.valueOf(this.hashCode()).compareTo(Integer.valueOf(((Position)o).hashCode()));
      } else {
        return -1;
      }
    }
  }
}


