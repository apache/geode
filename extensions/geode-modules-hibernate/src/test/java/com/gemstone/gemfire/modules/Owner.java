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
package com.gemstone.gemfire.modules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.annotations.Entity;


/**
 * Simple JavaBean domain object representing an owner.
 * 
 * @author Ken Krebs
 * @author Juergen Hoeller
 * @author Sam Brannen
 */
@javax.persistence.Entity
@Entity
public class Owner {
	private static final long serialVersionUID = 4315791692556052565L;

	@Column(name="address")
	private String address;

	private String city;

	private String telephone;

//	private Set<Pet> pets;
	@Id
	@GeneratedValue
	private Long id;
	
	private long versionNum = -1;

	public enum Status {
		NORMAL, PREMIUM
	};

	@Enumerated
	private Status status = Status.NORMAL;

	  private void setId(Long id) {
	    this.id = id;
	  }

	  public Long getId() {
	    return id;
	  }
	  
	public String getAddress() {
		return this.address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCity() {
		return this.city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getTelephone() {
		return this.telephone;
	}

	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}

	public long getVersionNum() {
		return versionNum;
	}

	public void setVersionNum(long versionNum) {
		this.versionNum = versionNum;
	}

	public Status getStatus() {
		return this.status;
	}

	public void setStatus(Status state) {
		if (state != null) {
			this.status = state;
		}
	}

//	protected void setPetsInternal(Set<Pet> pets) {
//		this.pets = pets;
//	}
//
//	protected Set<Pet> getPetsInternal() {
//		if (this.pets == null) {
//			this.pets = new HashSet<Pet>();
//		}
//		return this.pets;
//	}
//
//	public List<Pet> getPets() {
//		List<Pet> sortedPets = new ArrayList<Pet>(getPetsInternal());
//		PropertyComparator.sort(sortedPets, new MutableSortDefinition("name",
//				true, true));
//		return Collections.unmodifiableList(sortedPets);
//	}
//
//	public void addPet(Pet pet) {
//		getPetsInternal().add(pet);
//		pet.setOwner(this);
//	}
//
//	/**
//	 * Return the Pet with the given name, or null if none found for this Owner.
//	 * 
//	 * @param name
//	 *            to test
//	 * @return true if pet name is already in use
//	 */
//	public Pet getPet(String name) {
//		return getPet(name, false);
//	}
//
//	/**
//	 * Return the Pet with the given name, or null if none found for this Owner.
//	 * 
//	 * @param name
//	 *            to test
//	 * @return true if pet name is already in use
//	 */
//	public Pet getPet(String name, boolean ignoreNew) {
//		name = name.toLowerCase();
//		for (Pet pet : getPetsInternal()) {
//			if (!ignoreNew || !pet.isNew()) {
//				String compName = pet.getName();
//				compName = compName.toLowerCase();
//				if (compName.equals(name)) {
//					return pet;
//				}
//			}
//		}
//		return null;
//	}
//
//	@Override
//	public String toString() {
//		return new ToStringCreator(this).append("id", this.getId())
//				.append("new", this.isNew())
//				.append("lastName", this.getLastName())
//				.append("firstName", this.getFirstName())
//				.append("address", this.address).append("city", this.city)
//				.append("telephone", this.telephone)
//				.append("version", this.versionNum)
//				.append("status", this.status)
//
//				.toString();
//	}
}

