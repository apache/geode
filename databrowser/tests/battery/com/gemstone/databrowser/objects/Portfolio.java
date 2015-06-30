package com.gemstone.databrowser.objects;

import hydra.ConfigHashtable;
import hydra.Log;
import hydra.TestConfig;
import java.io.*;
import com.gemstone.gemfire.*; // for DataSerializable
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import objects.ConfigurableObject;
import com.gemstone.databrowser.Testutil;

/**
 * Portfolio ObjectType.
 *
 * @author vreddy
 *
 */
public class Portfolio extends positions implements Serializable,
    DataSerializable, ConfigurableObject {

  public int ID;

  public String pkid;

  public String type;

  public String status;

  public String description;

  public HashMap positions = new HashMap();

  public String[] names;

  public String secIds[];

  static ConfigHashtable conftab = TestConfig.tab();

  public String toString() {
    String out = "Portfolio [ID=" + ID + " status=" + status + " type=" + type
        + " pkid=" + pkid + " description=" + description + " names="
        + names.length + " secIds=" + secIds.length + "\n ";
    Iterator iter = positions.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry)iter.next();
      out += entry.getKey() + ":" + entry.getValue() + ", ";
    }
    return out + "\n]";
  }

  public int getID() {
    return ID;
  }

  public String getpkid() {
    return pkid;
  }

  public String[] getNames() {
    return names;
  }

  public String getStatus(){
    return this.status;
  }

  public String[] getSecIds() {
    return secIds;
  }

  public HashMap getPositions() {
    return positions;
  }

  public HashMap getPositions(String str) {
    return positions;
  }

  public HashMap getPositions(Integer i) {
    return positions;
  }

  public HashMap getPositions(int i) {
    return positions;
  }

  public String getType() {
    return this.type;
  }

  public boolean testMethod(boolean booleanArg) {
    return true;
  }

  public boolean isActive() {
    return status.equals("active");
  }

  /* public no-arg constructor required for Deserializable */
  public Portfolio() {
  }

  public boolean equals(Object o) {
    if (!(o instanceof Portfolio)) {
      return false;
    }
    Portfolio p2 = (Portfolio)o;
    return this.ID == p2.ID;
  }

  public int hashCode() {
    return this.ID;
  }

  public boolean boolFunction(String strArg) {
    if (strArg == "active") {
      return true;
    }
    else {
      return false;
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.ID = in.readInt();
    this.pkid = DataSerializer.readString(in);
    this.positions = (HashMap)DataSerializer.readObject(in);
    this.type = DataSerializer.readString(in);
    this.status = DataSerializer.readString(in);
    this.names = DataSerializer.readStringArray(in);
    this.secIds = DataSerializer.readStringArray(in);
    this.description = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.ID);
    DataSerializer.writeString(this.pkid, out);
    DataSerializer.writeObject(this.positions, out);
    DataSerializer.writeString(this.type, out);
    DataSerializer.writeString(this.status, out);
    DataSerializer.writeStringArray(this.names, out);
    DataSerializer.writeStringArray(this.secIds, out);
    DataSerializer.writeString(this.description, out);

  }

  public int getIndex() {
    return this.ID;
  }

  public void validate(int arg0) {
    // Do Nothing.
  }

  public void init(int index) {
    String s, nm, sIds;
    s = Testutil.getProperty("Portfolio.ID");
    if (s != null) {
      try {
        int i = Integer.parseInt(s.trim());
        this.ID = i;
      }
      catch (NumberFormatException nfe) {
        Log.getLogWriter().error(nfe);
      }
    }
    else {
      Log.getLogWriter().warning("Portfolio.ID property is not available.");
    }
    this.pkid = Testutil.getProperty("Portfolio.pkid");
    if (this.pkid == null) {
      Log.getLogWriter().warning("Portfolio.pkid property is not available.");
    }
    this.type = Testutil.getProperty("Portfolio.type");
    if (this.type == null) {
      Log.getLogWriter().warning("Portfolio.type property is not available.");
    }
    this.status = Testutil.getProperty("Portfolio.status");
    if (this.status == null) {
      Log.getLogWriter().warning("Portfolio.status property is not available.");
    }
    this.description = Testutil.getProperty("Portfolio.description");
    if (this.description == null) {
      Log.getLogWriter().warning(
          "Portfolio.description property is not available.");
    }

    nm = Testutil.getProperty("Portfolio.names");
    if (nm != null) {
      String[] allnames = nm.split(":");
      this.names = allnames;
    }
    else {
      Log.getLogWriter().warning("Portfolio.names property is not available.");
    }

    sIds = Testutil.getProperty("Portfolio.secIds");
    if (sIds != null) {
      String[] allSecIds = sIds.split(":");
      this.secIds = allSecIds;
    }
    else {
      Log.getLogWriter().warning("Portfolio.secIds property is not available.");
    }

    positions posns = new positions();
    posns.init(index);
    positions.put("ID", posns);
  }
}
