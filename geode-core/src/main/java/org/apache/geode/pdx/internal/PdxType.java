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
package org.apache.geode.pdx.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.pdx.PdxFieldAlreadyExistsException;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;

public class PdxType implements DataSerializable {  
  
  private static final long serialVersionUID = -1950047949756115279L;

  private int cachedHash = 0; 
  
  private int typeId;
  private String className;
  private boolean noDomainClass;
  /**
   * Will be set to true if any fields on this type have been deleted.
   * @since GemFire 8.1
   */
  private boolean hasDeletedField;

  /**
   * A count of the total number of variable length field offsets.
   */
  private int vlfCount;

  private final ArrayList<PdxField> fields = new ArrayList<PdxField>();

  private final transient Map<String, PdxField> fieldsMap = new HashMap<String, PdxField>();
  private transient volatile SortedSet<PdxField> sortedIdentityFields;
  
  public PdxType() {
    // for deserialization
  }
  
  public PdxType(String name, boolean expectDomainClass) {
    this.className = name;
    this.noDomainClass = !expectDomainClass;
    swizzleGemFireClassNames();
  }

  public PdxType(PdxType copy) {
    this.typeId = copy.typeId;
    this.className = copy.className;
    this.noDomainClass = copy.noDomainClass;
    this.vlfCount = copy.vlfCount;
    for (PdxField ft: copy.fields) {
      addField(ft);
    }
  }

  private static final byte NO_DOMAIN_CLASS_BIT = 1;
  private static final byte HAS_DELETED_FIELD_BIT = 2;

  private void swizzleGemFireClassNames() {
    OldClientSupportService svc = InternalDataSerializer.getOldClientSupportService();
    if (svc != null) {
      this.className = svc.processIncomingClassName(this.className);
    }
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.className = DataSerializer.readString(in);
    swizzleGemFireClassNames();
    {
      byte bits = in.readByte();
      this.noDomainClass = (bits & NO_DOMAIN_CLASS_BIT) != 0;
      this.hasDeletedField = (bits & HAS_DELETED_FIELD_BIT) != 0;
    }

    this.typeId = in.readInt();
    this.vlfCount = in.readInt();

    int arrayLen = InternalDataSerializer.readArrayLength(in);

    for (int i = 0; i < arrayLen; i++) {
      PdxField vft = new PdxField();
      vft.fromData(in);
      addField(vft);
    }
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.className, out);
    {
      // pre 8.1 we wrote a single boolean
      // 8.1 and after we write a byte whose bits are:
      // 1: noDomainClass
      // 2: hasDeletedField
      byte bits = 0;
      if (this.noDomainClass) {
        bits |= NO_DOMAIN_CLASS_BIT;
      }
      // Note that this code attempts to only set the HAS_DELETED_FIELD_BIT
      // if serializing for 8.1 or later.
      // But in some cases 8.1 serialized data may be sent to a pre 8.1 member.
      // In that case if this bit is set it will cause the pre 8.1 member
      // to set noDomainClass to true.
      // For this reason the pdx delete-field command should only be used after
      // all member have been upgraded to 8.1 or later.
      Version sourceVersion = InternalDataSerializer.getVersionForDataStream(out);
      if (sourceVersion.compareTo(Version.GFE_81) >= 0) {
        if (this.hasDeletedField) {
          bits |= HAS_DELETED_FIELD_BIT;
        }
      }
      out.writeByte(bits);
    }

    out.writeInt(this.typeId);
    out.writeInt(this.vlfCount);

    InternalDataSerializer.writeArrayLength(this.fields.size(), out);

    for (int i = 0; i < this.fields.size(); i++) {

      PdxField vft = this.fields.get(i);
      vft.toData(out);
    }
  }
   
  @Override
  public int hashCode() {
    int hash = cachedHash;
    if(hash == 0) {
      hash = 1;
      hash = hash * 31 + this.className.hashCode();
      for(PdxField field : this.fields) {
        hash = hash * 31 + field.hashCode();
      }
      if(hash == 0) {
        hash = 1;
      }
      cachedHash = hash;      
    }
    return cachedHash;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null || !(other instanceof PdxType)) {
      return false;
    }
    // Note: do not compare type id in equals
    PdxType otherVT = (PdxType)other;
    if (!(this.className.equals(otherVT.className))) {
      return false;
    }
    if (this.noDomainClass != otherVT.noDomainClass) {
      return false;
    }
    if (otherVT.fields.size() != this.fields.size()
        || otherVT.vlfCount != this.vlfCount) {
      return false;
    }
    for (int i = 0; i < this.fields.size(); i++) {
      if (!this.fields.get(i).equals(otherVT.fields.get(i))) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Return true if two pdx types have same class name and the same fields
   * but, unlike equals, field order does not matter.
   * Note a type that expects a domain class can be compatible with
   * one that does not expect a domain class.
   * @param other the other pdx type
   * @return true if two pdx types are compatible.
   */
  public boolean compatible(PdxType other) {
    if (other == null) return false;
    if(!getClassName().equals(other.getClassName())) {
      return false;
    }

    Collection<PdxField> myFields = getSortedFields();
    Collection<PdxField> otherFields = other.getSortedFields();

    return myFields.equals(otherFields);
  }

  public int getVariableLengthFieldCount() {
    return this.vlfCount;
  }
  
  public String getClassName() {
    return this.className;
  }
  
  public Class<?> getPdxClass() {
    try {
      return InternalDataSerializer.getCachedClass(getClassName());
    } catch (Exception e) {
      PdxSerializationException ex = new PdxSerializationException(
          LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_A_CLASS_0
          .toLocalizedString(getClassName()), e);
      throw ex;
    }
  }
  
  public boolean getNoDomainClass() {
    return this.noDomainClass;
  }
  
  public int getTypeId() {
    return this.typeId;
  }
  
  public int getDSId() {
    return this.typeId >> 24;
  }
  
  public int getTypeNum() {
    return this.typeId & 0x00FFFFFF;
  }
  
  public void setTypeId(int tId) {
    this.typeId = tId;
  }
 
  /*
   * This method is use to create Pdxtype for which classname is not available; while creating PdxInstance
   */
  public void setClassName(String className) {
    this.className = className;
  } 
  public void addField(PdxField ft) {
    if (this.fieldsMap.put(ft.getFieldName(), ft) != null) {
      throw new PdxFieldAlreadyExistsException("The field \"" + ft.getFieldName() + "\" already exists.");
    }
    this.fields.add(ft);
  }
  
  public void initialize(PdxWriterImpl writer) {
    this.vlfCount = writer.getVlfCount();
    int size = this.fields.size();
    int fixedLenFieldOffset = 0;
    boolean seenVariableLenType = false;
    for (int i = 0; i < size; i++) {
      PdxField vft = this.fields.get(i);
      // System.out.println(i + ": " + vft);
      if (vft.isVariableLengthType()) {
        if (seenVariableLenType) {
          vft.setVlfOffsetIndex(vft.getVarLenFieldSeqId());
        } else {
          vft.setRelativeOffset(fixedLenFieldOffset);
          vft.setVlfOffsetIndex(-1);
        }
        seenVariableLenType = true;
      } else if (seenVariableLenType) {
        PdxField tmp = null;
        int minusOffset = vft.getFieldType().getWidth();
        for (int j = (i+1); j < size; j++) {
          tmp = this.fields.get(j);
          if (tmp.isVariableLengthType()) {
            break;
          } else {
            minusOffset += tmp.getFieldType().getWidth();
          }
        }
        if (tmp != null && tmp.isVariableLengthType()) {
          vft.setRelativeOffset(-minusOffset);
          vft.setVlfOffsetIndex(tmp.getVarLenFieldSeqId());
        } else {
          vft.setRelativeOffset(-minusOffset);
          vft.setVlfOffsetIndex(-1); // From the byte after the last field data byte
        }
      } else {
        vft.setRelativeOffset(fixedLenFieldOffset);
        fixedLenFieldOffset += vft.getFieldType().getWidth();
      }
    }
    // no longer mark identity fields implicitly. Fixes bug 42976.

      // System.out.println("Printing the position array:");
      // for (int i = 0; i < this.positionArray.length; i++) {
      // System.out.println("[" + i + "][0]=" + this.positionArray[i][0] + ", ["
      // + i + "][1]=" + this.positionArray[i][1]);
      // }
  }

  public PdxField getPdxField(String fieldName) {
    PdxField result = this.fieldsMap.get(fieldName);
    if (result != null && result.isDeleted()) {
      result = null;
    }
    return result;
  }
  
  public List<PdxField> getFields() {
    return Collections.unmodifiableList(this.fields);
  }
  
  public PdxField getPdxFieldByIndex(int index) {
    return this.fields.get(index);
  }
  
  public int getFieldCount() {
    return this.fields.size();
  }
  public int getUndeletedFieldCount() {
    if (!getHasDeletedField()) {
      return 0;
    }
    int result = this.fields.size();
    for (PdxField f: this.fields) {
      if (f.isDeleted()) {
        result--;
      }
    }
    return result;
  }
  public String toFormattedString() {
    StringBuffer sb = new StringBuffer("PdxType[\n    ");
    sb.append("dsid=").append(getDSId());
    sb.append(", typenum=").append(getTypeNum());
    sb.append(", name=").append(this.className);
    sb.append(", fields=[");
    for (PdxField vft : fields) {
      sb.append("\n        ");
      sb.append(/*vft.getFieldName() + ":" + vft.getTypeId()*/ vft.toString());
    }
    sb.append("]]");
    return sb.toString();
  }

  public String toString() {
    StringBuffer sb = new StringBuffer("PdxType[");
    sb.append("dsid=").append(getDSId());
    sb.append(",typenum=").append(getTypeNum());
    sb.append(",name=").append(this.className);
    sb.append(",fields=[");
    for (PdxField vft : fields) {
      sb.append(/*vft.getFieldName() + ":" + vft.getTypeId()*/ vft.toString()).append(", ");
    }
    sb.append("]]");
    return sb.toString();
  }

  /**
   * 
   * @param readFields the fields that have been read
   * @return a List of fields that have not been read (may be empty).
   */
  public List<Integer> getUnreadFieldIndexes(List<String> readFields) {
    ArrayList<Integer> result = new ArrayList<Integer>();
    for (PdxField ft: this.fields) {
      if (!ft.isDeleted() && !readFields.contains(ft.getFieldName())) {
        result.add(ft.getFieldIndex());
      }
    }
    return result;
  }

  /**
   * Return true if the this type has a field that the other type does not have.
   * @param other the type we are comparing to
   * @return true if the this type has a field that the other type does not have.
   */
  public boolean hasExtraFields(PdxType other) {
    for (PdxField ft: this.fields) {
      if (!ft.isDeleted() && other.getPdxField(ft.getFieldName()) == null) {
        return true;
      }
    }
    return false;
  }
  
  // Result does not include deleted fields
  public SortedSet<PdxField> getSortedIdentityFields() {
    if(this.sortedIdentityFields == null) {
      TreeSet<PdxField> sortedSet = new TreeSet<PdxField>();
      for(PdxField field: fields) {
        if(field.isIdentityField() && !field.isDeleted()) {
          sortedSet.add(field);
        }
      }
      //If we don't find any marked identity fields, use all of the fields.
      if(sortedSet.isEmpty()) {
        for (PdxField field: fields) {
          if (!field.isDeleted()) {
            sortedSet.add(field);
          }
        }
      }
      this.sortedIdentityFields = sortedSet;
    }
    return this.sortedIdentityFields;
  }

  // Result does not include deleted fields
  public Collection<PdxField> getSortedFields() {
    TreeSet<PdxField> sortedSet = new TreeSet<PdxField>();
    for (PdxField pf: this.fields) {
      if (!pf.isDeleted()) {
        sortedSet.add(pf);
      }
    }
    return new ArrayList<PdxField>(sortedSet);
  }

  // Result does not include deleted fields
  public List<String> getFieldNames() {
    ArrayList<String> result = new ArrayList<String>(this.fields.size());
    for (PdxField f: this.fields) {
      if (!f.isDeleted()) {
        result.add(f.getFieldName());
      }
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Used to optimize auto deserialization
   */
  private transient final AtomicReference<AutoClassInfo> autoClassInfo = new AtomicReference<AutoClassInfo>();
  
  public void setAutoInfo(AutoClassInfo autoClassInfo) {
    this.autoClassInfo.set(autoClassInfo);
  }
  
  public AutoClassInfo getAutoInfo(Class<?> c) {
    AutoClassInfo ci = this.autoClassInfo.get();
    if (ci != null) {
      Class<?> lastClassAutoSerialized = ci.getInfoClass();
      if (c.equals(lastClassAutoSerialized)) {
        return ci;
      } else {
        if (lastClassAutoSerialized == null) {
          this.autoClassInfo.compareAndSet(ci, null);
        }
      }
    }
    return null;
  }

  public void toStream(PrintStream printStream, boolean printFields) {
    printStream.print("  ");
    printStream.print(getClassName());
    printStream.print(": ");
    printStream.print("id=");
    printStream.print(getTypeNum());
    if (getDSId() != 0) {
      printStream.print(" dsId=");
      printStream.print(getDSId());
    }
    printStream.println();
    if (printFields) {
      for (PdxField field: this.fields) {
        field.toStream(printStream);
      }
    }
  }

  public boolean getHasDeletedField() {
    return this.hasDeletedField;
  }
  public void setHasDeletedField(boolean b) {
    this.hasDeletedField = b;
  }
}
