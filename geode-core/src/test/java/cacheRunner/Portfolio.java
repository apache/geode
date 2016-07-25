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
package cacheRunner;

import java.util.*;
import java.io.Serializable;
import com.gemstone.gemfire.cache.Declarable;

/**
 * A stock portfolio that consists of multiple {@link Position}
 * objects that represent shares of stock (a "security").  Instances
 * of <code>Portfolio</code> can be stored in a GemFire
 * <code>Region</code> and their contents can be queried using the
 * GemFire query service.
 *
 * <P>
 *
 * This class is <code>Serializable</code> because we want it to be
 * distributed to multiple members of a distributed system.  Because
 * this class is <code>Declarable</code>, we can describe instances of
 * it in a GemFire <code>cache.xml</code> file.
 *
 * @since GemFire 4.0
 */
public class Portfolio implements Declarable, Serializable {
  private int id;  /* id is used as the entry key and is stored in the entry */
  private String type;
  private Map<String, Position> positions = new HashMap<String, Position>();
  private String status;
  
  /**
   * Initializes an instance of <code>Portfolio</code> from a
   * <code>Properties</code> object assembled from data residing in a
   * <code>cache.xml</code> file.
   */
  public void init(Properties props) {
    this.id = Integer.parseInt(props.getProperty("id"));
    this.type = props.getProperty("type", "type1");
    this.status = props.getProperty("status", "active");
    
    // get the positions. These are stored in the properties object
    // as Positions, not String, so use Hashtable protocol to get at them.
    // the keys are named "positionN", where N is an integer.
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith("position")) {
        Position pos = (Position) entry.getValue();
        this.positions.put(pos.getSecId(), pos);
      }
    }
  }
  
  /**
   * Returns the status of this portfolio (<code>active</code> or
   * <code>inactive</code>). 
   */
  public String getStatus(){
    return status;
  }
  
  /**
   * Returns the id of this portfolio.  When a <code>Portfolio</code>
   * placed in a GemFire <code>Region</code> entry, its id is used as
   * the key.
   */
  public int getId(){
    return this.id;
  }
  
  /**
   * Returns the positions held in this portfolio.
   *
   * @return a <code>Map</code> whose keys are the {@linkplain
   *         Position#getSecId security ids} and whose values are
   *         {@link Position} objects.
   */
  public Map<String, Position> getPositions(){
    return this.positions;
  }
  
  /**
   * Returns the type of this portfolio.  In this example, the type is
   * an arbitrary string.
   */
  public String getType() {
    return this.type;
  }
  
  /**
   * Returns whether or not this portfolio is active.
   */
  public boolean isActive(){
    return status.equals("active");
  }
  
  @Override
  public String toString(){
    StringBuffer out = new StringBuffer();
    out.append("\n\tPortfolio [id=" + this.id + " status=" + this.status);
    out.append(" type=" + this.type);
    
    Iterator<Map.Entry<String, Position>> iter = positions.entrySet().iterator();
    while(iter.hasNext()){
      Map.Entry<String, Position> entry = iter.next();
      out.append("\n\t\t");
      out.append(entry.getKey() + ":" + entry.getValue());
      if (iter.hasNext()) {
        out.append(", ");
      }
    }
    out.append("]");
    return out.toString();
  }
}
