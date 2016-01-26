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
package com.gemstone.gemfire.cache.query.internal.index;

import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MemoryIndexStoreWithInplaceModificationJUnitTest extends MemoryIndexStoreJUnitTest {
	
	public void subclassPreSetup() {
		IndexManager.INPLACE_OBJECT_MODIFICATION_FOR_TEST = true;
	}
	
	protected Region createRegion() {
		Region region = mock(LocalRegion.class);
		RegionAttributes ra = mock(RegionAttributes.class);
		when(region.getAttributes()).thenReturn(ra);
		when(ra.getInitialCapacity()).thenReturn(16);
		when(ra.getLoadFactor()).thenReturn(.75f);
		when(ra.getConcurrencyLevel()).thenReturn(16);
		return region;
	}
	
	@After
	public void resetInplaceModification() {
		IndexManager.INPLACE_OBJECT_MODIFICATION_FOR_TEST = false;
	}
	
	
}
