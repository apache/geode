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
package org.apache.geode.rest.internal.web.controllers;

import java.util.Date;

import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

/**
 * The Person class is an abstraction modeling a person.
 * <p/>
 *
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class Person implements PdxSerializable {

  private static final long serialVersionUID = 42108163264l;

  protected static final String DOB_FORMAT_PATTERN = "MM/dd/yyyy";

  private Long id;

  private Date birthDate;

  private Gender gender;

  private String firstName;
  private String middleName;
  private String lastName;

  public Person() {}

  public Person(final Long id) {
    this.id = id;
  }

  public Person(final String firstName, final String lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
  }

  public Person(Long id, String fn, String mn, String ln, Date bDate, Gender g) {
    this.id = id;
    this.firstName = fn;
    this.middleName = mn;
    this.lastName = ln;
    this.birthDate = bDate;
    this.gender = g;
  }

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(final String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(final String lastName) {
    this.lastName = lastName;
  }

  public String getMiddleName() {
    return middleName;
  }

  public void setMiddleName(final String middleName) {
    this.middleName = middleName;
  }

  public Date getBirthDate() {
    return birthDate;
  }

  public void setBirthDate(final Date birthDate) {
    this.birthDate = birthDate;
  }

  public Gender getGender() {
    return gender;
  }

  public void setGender(final Gender gender) {
    this.gender = gender;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof Person)) {
      return false;
    }

    final Person that = (Person) obj;

    return (ObjectUtils.equals(this.getId(), that.getId())
        || (ObjectUtils.equals(this.getBirthDate(), that.getBirthDate())
            && ObjectUtils.equals(this.getLastName(), that.getLastName())
            && ObjectUtils.equals(this.getFirstName(), that.getFirstName())));
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getId());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getBirthDate());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getLastName());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getFirstName());
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("{ type = ");
    buffer.append(getClass().getName());
    buffer.append(", id = ").append(getId());
    buffer.append(", firstName = ").append(getFirstName());
    buffer.append(", middleName = ").append(getMiddleName());
    buffer.append(", lastName = ").append(getLastName());
    buffer.append(", birthDate = ")
        .append(DateTimeUtils.format(getBirthDate(), DOB_FORMAT_PATTERN));
    buffer.append(", gender = ").append(getGender());
    buffer.append(" }");
    return buffer.toString();
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeString("@type", getClass().getName());
    writer.writeLong("id", id);
    writer.writeString("firstName", firstName);
    writer.writeString("middleName", middleName);
    writer.writeString("lastName", lastName);
    writer.writeObject("gender", gender);
    writer.writeDate("birthDate", birthDate);

  }

  @Override
  public void fromData(PdxReader reader) {
    String type = reader.readString("@type");
    id = reader.readLong("id");
    firstName = reader.readString("firstName");
    middleName = reader.readString("middleName");
    lastName = reader.readString("lastName");
    gender = (Gender) reader.readObject("gender");
    birthDate = reader.readDate("birthDate");

  }

}
