package ittest.io.pivotal.gemfire.spark.connector;

import java.io.Serializable;
import java.util.Properties;
import com.gemstone.gemfire.cache.Declarable;

/**
 * Represents a number of shares of a stock ("security") held in a {@link
 * Portfolio}.
 * </p>
 * This class is <code>Serializable</code> because we want it to be distributed
 * to multiple members of a distributed system.  Because this class is
 * <code>Declarable</code>, we can describe instances of it in a GemFire
 * <code>cache.xml</code> file.
 * </p>
 *
 */
public class Position implements Declarable, Serializable {

  private static final long serialVersionUID = -8229531542107983344L;

  private String secId;
  private double qty;
  private double mktValue;

  public Position(Properties props) {
    init(props);
  }

  @Override
  public void init(Properties props) {
    this.secId = props.getProperty("secId");
    this.qty = Double.parseDouble(props.getProperty("qty"));
    this.mktValue = Double.parseDouble(props.getProperty("mktValue"));
  }

  public String getSecId(){
    return this.secId;
  }

  public double getQty(){
    return this.qty;
  }

  public double getMktValue() {
    return this.mktValue;
  }

  @Override
  public String toString(){
    return new StringBuilder()
            .append("Position [secId=").append(secId)
            .append(" qty=").append(this.qty)
            .append(" mktValue=").append(mktValue).append("]").toString();
  }
}

