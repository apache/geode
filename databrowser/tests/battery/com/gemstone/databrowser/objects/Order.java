package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Order ObjectType.
 * 
 * @author vreddy
 * 
 */
public class Order implements DataSerializable {

  public int ord_id;

  public Date ord_date;

  public String ord_status;

  public String ord_cust_id;

  public Order() {

  }

  public Order(int ord_id, Date ord_date, String ord_status, String ord_cust_id) {
    this.ord_id = ord_id;
    this.ord_date = ord_date;
    this.ord_status = ord_status;
    this.ord_cust_id = ord_cust_id;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.ord_id = DataSerializer.readPrimitiveInt(in);
    this.ord_date = DataSerializer.readDate(in);
    this.ord_status = DataSerializer.readString(in);
    this.ord_cust_id = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.ord_id, out);
    DataSerializer.writeDate(this.ord_date, out);
    DataSerializer.writeString(this.ord_status, out);
    DataSerializer.writeString(this.ord_cust_id, out);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Order [ ord_id = " + this.ord_id + " , ord_date = "
        + this.ord_date + " , ord_status =" + this.ord_status
        + " , ord_cust_id =" + this.ord_cust_id + "]");
    return buffer.toString();
  }
}
