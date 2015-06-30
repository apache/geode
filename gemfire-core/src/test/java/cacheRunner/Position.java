package cacheRunner;

import java.util.*;
import java.io.Serializable;
import com.gemstone.gemfire.cache.Declarable;

/**
 * Represents a number of shares of a stock ("security") held in a
 * {@link Portfolio}.
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
public class Position implements Declarable, Serializable {
  private String secId;
  private double qty;
  private double mktValue;
  
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

  @Override
  public String toString(){
    return "Position [secId="+secId+" qty="+this.qty+" mktValue="+mktValue+"]";
  }

}
