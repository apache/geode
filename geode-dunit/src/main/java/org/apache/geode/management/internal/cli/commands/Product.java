/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import java.io.Serializable;
import java.util.TreeMap;

public class Product implements Serializable/* , PdxSerializable */ {
  Long productID;

  public long getProductID() {
    return productID;
  }

  public void setProductID(Long productID) {
    this.productID = productID;
  }

  public TreeMap<String, String> getProductCodes() {
    return productCodes;
  }

  public void setProductCodes(TreeMap<String, String> productCodes) {
    this.productCodes = productCodes;
  }

  public String getContractSize() {
    return contractSize;
  }

  public void setContractSize(String contractSize) {
    this.contractSize = contractSize;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  TreeMap<String, String> productCodes;
  String contractSize;
  String status;

  public Product(Long productID, TreeMap<String, String> productCodes, String contractSize,
      String status) {
    this.productID = productID;
    this.productCodes = productCodes;
    this.contractSize = contractSize;
    this.status = status;
  }

  public Product() {}

  @Override
  public String toString() {
    return "Product{" +
        "productID=" + productID +
        ", productCodes=" + productCodes +
        ", contractSize='" + contractSize + '\'' +
        ", status='" + status + '\'' +
        '}';
  }
}
