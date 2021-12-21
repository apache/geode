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
/*
 * Created on Apr 18, 2005
 *
 *
 */
package org.apache.geode.cache.query.internal.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.cache.query.IndexType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 *
 * This class contains the information needed to create an index It will contain the callback data
 * between <index></index> invocation
 */
public class IndexCreationData implements DataSerializableFixedID {

  private String name = null;
  private IndexType indexType = null;
  private String fromClause = null;
  private String expression = null;
  private String importStr = null;
  private PartitionedIndex partitionedIndex = null;
  private boolean loadEntries = false;

  public IndexCreationData() {

  }

  public IndexCreationData(String name) {
    this.name = name;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexData(IndexType type, String fromClause, String expression, String importStr) {
    indexType = type;
    this.fromClause = fromClause;
    this.expression = expression;
    this.importStr = importStr;
  }

  public void setIndexData(IndexType type, String fromClause, String expression, String importStr,
      boolean loadEntries) {
    indexType = type;
    this.fromClause = fromClause;
    this.expression = expression;
    this.importStr = importStr;
    this.loadEntries = loadEntries;
  }

  public void setPartitionedIndex(PartitionedIndex index) {
    partitionedIndex = index;
  }

  public PartitionedIndex getPartitionedIndex() {
    return partitionedIndex;
  }

  public String getIndexFromClause() {
    return fromClause;
  }

  public String getIndexExpression() {
    return expression;
  }

  public String getIndexImportString() {
    return importStr;
  }

  public String getIndexName() {
    return name;
  }

  public boolean loadEntries() {
    return loadEntries;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return INDEX_CREATION_DATA;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeUTF(name);
    out.writeUTF(expression);
    out.writeUTF(fromClause);
    out.writeBoolean(loadEntries);

    if (IndexType.PRIMARY_KEY == indexType) {
      out.writeByte(0);
    } else if (IndexType.HASH == indexType) {
      out.writeByte(1);
    } else {
      out.writeByte(2);
    }


    if (importStr != null) {
      out.writeBoolean(true);
      out.writeUTF(importStr);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    name = in.readUTF();
    expression = in.readUTF();
    fromClause = in.readUTF();
    loadEntries = in.readBoolean();

    byte byteIndexType = in.readByte();
    if (0 == byteIndexType) {
      indexType = IndexType.PRIMARY_KEY;
    } else if (1 == byteIndexType) {
      indexType = IndexType.HASH;
    } else {
      indexType = IndexType.FUNCTIONAL;
    }

    boolean isImportStr = in.readBoolean();
    if (isImportStr) {
      importStr = in.readUTF();
    }
  }

}
