package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Shipment ObjectType.
 * 
 * @author vreddy
 * 
 */
public class Shipment implements DataSerializable {

  int ship_status;

  Address destination;

  int priority;

  String contact;

  public Shipment() {

  }

  public Shipment(int ship_status, Address destination, int priority,
      String contact) {
    super();
    this.ship_status = ship_status;
    this.destination = destination;
    this.priority = priority;
    this.contact = contact;
  }

  public String getContact() {
    return contact;
  }

  public void setContact(String contact) {
    this.contact = contact;
  }

  public Address getDestination() {
    return destination;
  }

  public void setDestination(Address destination) {
    this.destination = destination;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public int getShip_status() {
    return ship_status;
  }

  public void setShip_status(int ship_status) {
    this.ship_status = ship_status;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.ship_status = DataSerializer.readPrimitiveInt(in);
    this.destination = (Address)DataSerializer.readObject(in);
    this.priority = DataSerializer.readPrimitiveInt(in);
    this.contact = DataSerializer.readString(in);

  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.ship_status, out);
    DataSerializer.writeObject(this.destination, out);
    DataSerializer.writePrimitiveInt(this.priority, out);
    DataSerializer.writeString(this.contact, out);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Shipment [ ship_status = " + this.ship_status
        + " , destination = " + this.destination + " , priority ="
        + this.priority + " , contact =" + this.contact + "]");
    return buffer.toString();
  }
}
