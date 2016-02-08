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
package parReg.query.unittest;

import hydra.Log;

import java.io.Serializable;
import java.util.*;

/**
 * A version of the Portfolio Object used for query. 
 */
public class NewPortfolio implements Serializable {
  
  protected String myVersion;

  protected static final Random rng = new Random();

  protected int NUM_OF_TYPES = 10;
  protected int MAX_NUM_OF_POSITIONS = 5;     
  protected int NUM_OF_SECURITIES = 200;
  private int MAX_QTY = 100;    //max is 100*100 
  private int MAX_PRICE = 100;
  protected int id = 0;           
  protected String name = "name";         //key value, needs to be unique
  protected String status = "status";
  protected String type = "type";
  protected Map positions = new HashMap();
  public String undefinedTestField = null;
  
  public NewPortfolio() {
    //use default
    myVersion = "tests/parReg.query.NewPortfolio";
  }
  
  /**
   * Constructor to randomly populate the portfolio.
   * @param name
   * @param id
   */
  public NewPortfolio(String name, int id) {
    myVersion = "tests/parReg.query.NewPortfolio";
    this.name = name;
    this.id = id;
    
    this.status = id % 2 == 0 ? "active" : "inactive";
    this.type = "type" + (id % NUM_OF_TYPES);
    
    setPositions();
  }
  
  public int getId() {
    return id;
  }
  
  public String getName() {
    return name;
  }
  
  public String getStatus() {
    return status;
  }
  
  public String getType() {
    return type;
  }
  
  public void setId(int id) {
    this.id = id;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public void  setStatus(String status) {
    this.status = status;
  }
  
  public void setType(String type) {
    this.type = type;
  }
    
  public void init( int i ) {
    this.name = new Integer(i).toString();
    this.id = i;
    this.status = i % 2 == 0 ? "active" : "inactive";
    this.type = "type" + (i % NUM_OF_TYPES);
    
    setPositions();

  }
  
  private void setPositions() {
    int numOfPositions = rng.nextInt(MAX_NUM_OF_POSITIONS);
    if (numOfPositions == 0) 
      numOfPositions++;
     
    int secId =  rng.nextInt(NUM_OF_SECURITIES);
    
    for (int i=0; i < numOfPositions; i++) {
      Properties props = getProps();
      
//    secId needs to be UNIQUE in one portfolio, keep track MAX_NUM_OF_POSITIONS and NUM_OF_SECURITIES
      secId += i * 7;                    
      if (secId > NUM_OF_SECURITIES)
        secId -= NUM_OF_SECURITIES;
      props.setProperty("secId", new Integer(secId).toString());
      
      Position pos = new Position();
      pos.init(props);
      this.positions.put(pos.getSecId(), pos);
    }
  }
  
  public void validate( int index ) {
    //do nothing
  }
  
  public int getIndex() {
    return this.id;
  }
  
  public Map getPositions(){
    return positions;
  }
  
  /**
   * To provide random values to populate a position.
   * @return
   */
  protected Properties getProps() {
   Properties props = new Properties();
   Double qty = new Double(rng.nextInt(MAX_QTY) * 100.00);
   Double mktValue = new Double(rng.nextDouble() * MAX_PRICE); 

   props.setProperty("qty", qty.toString());
   props.setProperty("mktValue", mktValue.toString());

   return props;
  }
  
  /**
   * To enable the comparison.
   */
  public boolean equals(Object anObj) {
    
    if (anObj == null) {
       return false;
    }
//    Log.getLogWriter().info("comparing\n"+this+"\n and "+anObj);

    if (anObj.getClass().getName().equals(this.getClass().getName())) { // cannot do class identity check for pdx tets
//      Log.getLogWriter().info("checkpoint 1,.this class is checked " + this.getClass().getName() );
       NewPortfolio np = (NewPortfolio)anObj;
       if (!np.name.equals(this.name) || (np.id != this.id) || !np.type.equals(this.type) || !np.status.equals(this.status)) {
//         Log.getLogWriter().info("checkpoint 1,obj " +np.name + " " + np.id + " " + np.type );
         return false;
       }
//       Log.getLogWriter().info("checkpoint 2, NP name, id checked" );
       
       if (np.positions == null) {
          if (this.positions != null) {
            return false;
          }
       } else {
//         Log.getLogWriter().info("checkpoint 3, checking position size" );
         if (np.positions.size() != this.positions.size()) {
           Log.getLogWriter().info("checkpoint 3, position size failed" );
           return false;
         }
         else {                 //loops thru the map of positions
           Iterator itr = np.positions.values().iterator();
           Position pos;
           while (itr.hasNext()) {
//             Log.getLogWriter().info("checkpoint 4, to check iteration" );
             pos = (Position)itr.next();
//             Log.getLogWriter().info("checkpoint 4, to check pos" );
             if (!this.positions.containsValue(pos)){
//               Log.getLogWriter().info("checkpoint 5, check pos failed" );                                            
               return false;
             }            
           }
         }
       }
    } else {
      //not same class
//      Log.getLogWriter().info("checkpoint 6, not the same class");
       return false;
    }
    return true;
 }

  public int hashCode() {
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + status.hashCode();
    result = 37 * result + type.hashCode();
    result = 37 * result + id;
    result = 37 * result + positions.hashCode();
    
    return result;
  }
 
  /** Create a map of fields and field values to use to write to the blackboard
   *  since PdxSerialiables cannot be put on the blackboard since the MasterController
   *  does not have pdx objects on its classpath. For PdxSerializables
   *  we put this Map on the blackboard instead.
   */
  public Map createPdxHelperMap() {
    Map fieldMap = new HashMap();
    fieldMap.put("className", this.getClass().getName());
    fieldMap.put("myVersion", myVersion);
    fieldMap.put("id", id);
    fieldMap.put("name", name);
    fieldMap.put("status", status);
    fieldMap.put("type", type);
    fieldMap.put("positions", positions);
    fieldMap.put("undefinedTestField", undefinedTestField);
//    Log.getLogWriter().info("created map in tests/parReg.query.NewPortfolio: " + fieldMap);
    return fieldMap;
  }

  /** Restore the fields of this instance using the values of the Map, created
   *  by createPdxHelperMap()
   */
  public void restoreFromPdxHelperMap(Map aMap) {
//    Log.getLogWriter().info("restoring from map into " + this.getClass().getName() + ": " + aMap);
    this.myVersion = (String)aMap.get("myVersion");
    this.id = (Integer)aMap.get("id");
    this.name = (String)aMap.get("name");
    this.status = (String)aMap.get("status");
    this.type = (String)aMap.get("type");
    this.positions = (Map)aMap.get("positions");
    this.undefinedTestField = (String)aMap.get("undefinedTestField");
//    Log.getLogWriter().info("returning instance from map in tests/parReg.query.NewPortfolio: " + this);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("NewPortfolio [ID=" + this.id + " status=" + status);
    sb.append(" name=" + this.name);
    
    Iterator iter = positions.entrySet().iterator();
    sb.append(" Positions:[ ");
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      sb.append(entry.getKey() + ":" + entry.getValue() + ", ");
    }
    sb.append("] ]");
    return sb.toString();
  }

}

  

