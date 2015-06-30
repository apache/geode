package com.gemstone.gemfire.tools.databrowser.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class Position implements DataSerializable {

  private String secId;
  private double mktValue;
  private double qty;
  
  public Position() {
    
  }
    
  public Position(String secId, double mktValue, double qty ) {
    super();
    this.mktValue = mktValue;
    this.qty = qty;
    this.secId = secId;
  }

  public String getSecId() {
    return secId;
  }

  public void setSecId(String secId) {
    this.secId = secId;
  }

  public double getMktValue() {
    return mktValue;
  }

  public void setMktValue(double mktValue) {
    this.mktValue = mktValue;
  }

  public double getQty() {
    return qty;
  }

  public void setQty(double qty) {
    this.qty = qty;
  }


  public void fromData(DataInput arg0) throws IOException,
      ClassNotFoundException {
    this.secId = DataSerializer.readString(arg0);
    this.mktValue = DataSerializer.readPrimitiveDouble(arg0);
    this.qty = DataSerializer.readPrimitiveDouble(arg0);
  }


  public void toData(DataOutput arg0) throws IOException {
    DataSerializer.writeString(this.secId, arg0);
    DataSerializer.writePrimitiveDouble(this.mktValue, arg0);
    DataSerializer.writePrimitiveDouble(this.qty , arg0);
  }

}
