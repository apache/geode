package com.gemstone.gemfire.tools.databrowser.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class Portfolio implements DataSerializable {

  String id; /* This id is used as the entry key and is stored here */
  String type;
  String status;
  HashMap<String, Position> positions; /* The value is the Position, with secId used as the key*/
    
  public Portfolio() {
    
  }  
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
  
  public HashMap<String, Position> getPositions() {
    return positions;
  }
  
  public void setPositions(HashMap<String, Position> positions) {
    this.positions = positions;
  }

  public Portfolio(String id, HashMap<String, Position> positions,
      String status, String type) {
    super();
    this.id = id;
    this.positions = positions;
    this.status = status;
    this.type = type;
  }


  public void fromData(DataInput arg0) throws IOException,
      ClassNotFoundException {
    this.id = DataSerializer.readString(arg0);
    this.type = DataSerializer.readString(arg0);
    this.status = DataSerializer.readString(arg0);
    this.positions = DataSerializer.readHashMap(arg0);
  }


  public void toData(DataOutput arg0) throws IOException {
    DataSerializer.writeString(this.id, arg0);   
    DataSerializer.writeString(this.id, arg0);
    DataSerializer.writeString(this.id, arg0);
    DataSerializer.writeHashMap(this.positions, arg0);
  }

}
