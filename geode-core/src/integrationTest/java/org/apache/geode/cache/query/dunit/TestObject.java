package org.apache.geode.cache.query.dunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class TestObject implements DataSerializable {
  protected String _ticker;
  protected int _price;
  public int id;
  public int important;
  public int selection;
  public int select;

  public TestObject() {}

  public TestObject(int id, String ticker) {
    this.id = id;
    this._ticker = ticker;
    this._price = id;
    this.important = id;
    this.selection = id;
    this.select = id;
  }

  public int getId() {
    return this.id;
  }

  public String getTicker() {
    return this._ticker;
  }

  public int getPrice() {
    return this._price;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    DataSerializer.writeString(this._ticker, out);
    out.writeInt(this._price);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this._ticker = DataSerializer.readString(in);
    this._price = in.readInt();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("TestObject [").append("id=").append(this.id).append("; ticker=")
        .append(this._ticker).append("; price=").append(this._price).append("]");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    TestObject other = (TestObject) o;
    if ((id == other.id) && (_ticker.equals(other._ticker))) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.id;
  }

}
