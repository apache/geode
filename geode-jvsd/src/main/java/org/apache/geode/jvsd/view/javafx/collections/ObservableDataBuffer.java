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
package org.apache.geode.jvsd.view.javafx.collections;

import javafx.collections.ObservableListBase;

import org.apache.geode.jvsd.view.javafx.scene.chart.Data;

public class ObservableDataBuffer<X, Y> extends ObservableListBase<Data<X, Y>> {

  @Override
  public Data<X, Y> get(int index) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public boolean add(Data<X, Y> e) {
    
    
    return true;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

}
