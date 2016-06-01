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
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.json.TypedJson;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class TypedJsonJUnitTest {

	public enum Currency {
		PENNY, NICKLE, DIME, QUARTER
	};
	
	private static final String RESULT = "result";
	
	
	public void checkResult(TypedJson tjson){
		
		GfJsonObject gfJsonObj;
		try {
			gfJsonObj = new GfJsonObject(tjson.toString());
			System.out.println(gfJsonObj);
			assertNotNull(gfJsonObj.get(RESULT));
		} catch (GfJsonException e) {
			fail("Result could not be found");
		}
		
	}

  @Test
	public void testArrayList() {

		List<String> ls = new ArrayList<String>();
		ls.add("ONE");
		ls.add("TWO");
		ls.add("THREE");
		TypedJson tjson = new TypedJson(RESULT,ls);

		checkResult(tjson);
	}

  @Test
	public void testArray() {

		int[] arr = new int[3];
		for (int i = 0; i < 3; i++) {
			arr[i] = i;
		}
		TypedJson tjson = new TypedJson(RESULT,arr);
		checkResult(tjson);
		
	}

  @Test
	public void testBigList() {

		List<String> ls = new ArrayList<String>();
		for (int i = 0; i < 1000; i++) {
			ls.add("BIG_COLL_" + i);
		}
		TypedJson tjson = new TypedJson(RESULT,ls);
		checkResult(tjson);
	}

  @Test
	public void testEnum() {
	  EnumContainer test = new EnumContainer(Currency.DIME);
		TypedJson tjson = new TypedJson(RESULT,test);
		checkResult(tjson);
		TypedJson enumObj = new TypedJson(RESULT,Currency.DIME);
		checkResult(enumObj);

	}

  @Test
	public void testEnumList() {
		List ls = new ArrayList();
		ls.add(Currency.DIME);
		ls.add(Currency.NICKLE);
		ls.add(Currency.QUARTER);
		ls.add(Currency.NICKLE);
		TypedJson tjson = new TypedJson(RESULT,ls);
		System.out.println(tjson);

	}

  @Test
	public void testMap() {
		Map<String, String> testMap = new HashMap<String, String>();
		testMap.put("1", "ONE");
		testMap.put("2", "TWO");
		testMap.put("3", "THREE");
		testMap.put("4", "FOUR");
		TypedJson tjson = new TypedJson(RESULT,testMap);
		System.out.println(tjson);

	}

  @Test
	public void testBigDecimal() {
		java.math.BigDecimal dc = new java.math.BigDecimal(20);
		TypedJson tjson = new TypedJson(RESULT,dc);
		System.out.println(tjson);

	}
	
  @Test
	public void testUserObject() {
		Portfolio p = new Portfolio(2);
		TypedJson tjson = new TypedJson(RESULT,p);
		System.out.println(tjson);

	}

  @Test
	public void testObjects() {
		Object obj = new Object();
		TypedJson tjson = new TypedJson(RESULT,obj);
		System.out.println(tjson);
	}

  @Test
	public void testPDXObject() {
		final Properties props = new Properties();
		props.setProperty(MCAST_PORT, "0");
		DistributedSystem.connect(props);
		Cache cache = new CacheFactory().create();
		PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
		Portfolio p = new Portfolio(2);
		pf.writeInt("ID", 111);
		pf.writeString("status", "active");
		pf.writeString("secId", "IBM");
		pf.writeObject("portfolio", p);
		PdxInstance pi = pf.create();

		TypedJson tJsonObj = new TypedJson(RESULT,pi);
		System.out.println(tJsonObj);
		cache.close();

	}

  @Test
	public void testNestedPDXObject() {
		final Properties props = new Properties();
		props.setProperty(MCAST_PORT, "0");
		DistributedSystem.connect(props);
		Cache cache = new CacheFactory().create();

		PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);

		pf.writeInt("ID", 111);
		pf.writeString("status", "active");
		pf.writeString("secId", "IBM");
		PdxInstance pi = pf.create();

		PDXContainer cont = new PDXContainer(1);
		cont.setPi(pi);

		TypedJson tJsonObj = new TypedJson(RESULT,cont);
		System.out.println(tJsonObj);
		cache.close();

	}

  @Test
	public void testUserObjectArray() {
		Portfolio[] p = createPortfoliosAndPositions(2);
		TypedJson t1 = new TypedJson(RESULT,p);
		System.out.println(t1);
	}

  @Test
	public void testMemUsage() {
		Portfolio[] p = createPortfoliosAndPositions(1000);
		System.out.println("Size Of port " + ObjectSizer.REFLECTION_SIZE.sizeof(p));
		TypedJson t1 = new TypedJson(RESULT,p);
		System.out.println("Size Of json " + ObjectSizer.REFLECTION_SIZE.sizeof(t1));

	}

	
  @Test
	public void testQueryLike(){
		Portfolio[] p = createPortfoliosAndPositions(2);
		TypedJson t1 = new TypedJson(RESULT,null);
		t1.add("member", "server1");
		System.out.println(t1);
		for(int i = 0; i< 2 ;i++){
				t1.add(RESULT,p[i]);
			
		}
		System.out.println(t1);


	}

	private static class EnumContainer {

		Currency curr;

		public EnumContainer(Currency curr) {
			this.curr = curr;
		}

		public Currency getCurr() {
			return curr;
		}

		public void setCurr(Currency curr) {
			this.curr = curr;
		}

	}

	private static class PDXContainer {
		PdxInstance pi;
		int counter;

		public PDXContainer(int count) {
			this.counter = count;
		}

		public PdxInstance getPi() {
			return pi;
		}

		public void setPi(PdxInstance pi) {
			this.pi = pi;
		}

		public int getCounter() {
			return counter;
		}

		public void setCounter(int counter) {
			this.counter = counter;
		}
	}

	public Portfolio[] createPortfoliosAndPositions(int count) {
		Position.cnt = 0; // reset Portfolio counter
		Portfolio[] portfolios = new Portfolio[count];
		for (int i = 0; i < count; i++) {
			portfolios[i] = new Portfolio(i);
		}
		return portfolios;
	}

}
