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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Apache.Geode.Client.FwkLib  
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client.Tests.NewAPI;
  using Apache.Geode.Client;
  

//[Serializable]
//  public class TxInfo<Tkey,TVal>
//  {
//    TransactionId txId;
//    int numExecutions;
//    String regionName;
//    Object key;
//    //ModRoutingObject routingObject;

//    public TxInfo()
//    {
//      this.numExecutions = 0;
//    }

//    public String getTxId()
//    {
//      return this.txId;
//    }

//    public int getNumExecutions()
//    {
//      return this.numExecutions;
//    }

//    public String getRegionName()
//    {
//      return this.regionName;
//    }

//    public Object getKey()
//    {
//      return this.key;
//    }

//    //public ModRoutingObject getRoutingObject()
//    //{
//    //  return this.routingObject;
//    //}

//    public void setTxId(String txId)
//    {
//      this.txId = txId;
//    }

//    public void setNumExecutions(int executions)
//    {
//      this.numExecutions = executions;
//    }

//    public void incrementNumExecutions()
//    {
//      //this.numExecutions = numExecutions++;
//      setNumExecutions(this.numExecutions++);
//      Util.Log("Inside TXInfo numExecutionis {0}", this.numExecutions++);

//    }

//    public void setRegionName(String regionName)
//    {
//      this.regionName = regionName;
//    }

//    public void setKey(Object aKey)
//    {
//      this.key = aKey;
//    }

//    //public void setRoutingObject(ModRoutingObject aRoutingObject)
//    //{
//    //  this.routingObject = aRoutingObject;
//    //}

//    public String toString()
//    {
//      string aStr = "";
//      //StringBuffer aStr = new StringBuffer();
//      //aStr.append("TxInfo {" + txId + ", numExecutions: " + numExecutions + ", FilterInfo {" + regionName + ", " + key + ", " + routingObject + "}");
//      return aStr;
//    }
//  }
  [Serializable]
  public class TxInfo //: IPdxSerializable
  {
    TransactionId txId;
   // Object txId;
    int numExecutions;
    //String regionName;
     
    public TxInfo()
     {
       this.numExecutions = 0;
     }

    public TransactionId getTxId()
    {
      return this.txId;
    }

    //public Object getTxId()
    //{
    //  return this.txId;
    //}
     public int getNumExecutions()
     {
       return this.numExecutions;
     }

     //public String getRegionName()
     //{
     //  return this.regionName;
     //}

    public void setTxId(TransactionId txId)
    {
      this.txId = txId;
    }

    //public void setTxId(Object txId)
    //{
    //  this.txId = txId;
    //}

     public void setNumExecutions(int executions)
     {
       this.numExecutions = executions;
     }

     public void incrementNumExecutions()
     {
       //this.numExecutions = numExecutions++;
       setNumExecutions(this.numExecutions++);
       Util.Log("Inside TXInfo numExecutionis {0}", this.numExecutions++);

     }
  
  }
}

