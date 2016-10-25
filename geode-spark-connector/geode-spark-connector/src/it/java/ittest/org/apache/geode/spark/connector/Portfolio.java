/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ittest.org.apache.geode.spark.connector;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.geode.cache.Declarable;

/**
 * A stock portfolio that consists of multiple {@link Position} objects that
 * represent shares of stock (a "security").  Instances of
 * <code>Portfolio</code> can be stored in a Geode <code>Region</code> and
 * their contents can be queried using the Geode query service.
 * </p>
 * This class is <code>Serializable</code> because we want it to be distributed
 * to multiple members of a distributed system.  Because this class is
 * <code>Declarable</code>, we can describe instances of it in a Geode
 * <code>cache.xml</code> file.
 * </p>
 *
 */
public class Portfolio implements Declarable, Serializable {

  private static final long serialVersionUID = 9097335119586059309L;

  private int id;  /* id is used as the entry key and is stored in the entry */
  private String type;
  private Map<String,Position> positions = new LinkedHashMap<String,Position>();
  private String status;

  public Portfolio(Properties props) {
    init(props);
  }

  @Override
  public void init(Properties props) {
    this.id = Integer.parseInt(props.getProperty("id"));
    this.type = props.getProperty("type", "type1");
    this.status = props.getProperty("status", "active");

    // get the positions. These are stored in the properties object
    // as Positions, not String, so use Hashtable protocol to get at them.
    // the keys are named "positionN", where N is an integer.
    for (Map.Entry<Object, Object> entry: props.entrySet()) {
      String key = (String)entry.getKey();
      if (key.startsWith("position")) {
        Position pos = (Position)entry.getValue();
        this.positions.put(pos.getSecId(), pos);
      }
    }
  }

  public void setType(String t) {this.type = t; }

  public String getStatus(){
    return status;
  }

  public int getId(){
    return this.id;
  }

  public Map<String,Position> getPositions(){
    return this.positions;
  }

  public String getType() {
    return this.type;
  }

  public boolean isActive(){
    return status.equals("active");
  }

  @Override
  public String toString(){
    StringBuilder buf = new StringBuilder();
    buf.append("\n\tPortfolio [id=" + this.id + " status=" + this.status);
    buf.append(" type=" + this.type);
    boolean firstTime = true;
    for (Map.Entry<String, Position> entry: positions.entrySet()) {
      if (!firstTime) {
        buf.append(", ");
      }
      buf.append("\n\t\t");
      buf.append(entry.getKey() + ":" + entry.getValue());
      firstTime = false;
    }
    buf.append("]");
    return buf.toString();
  }
}

