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
package com.gemstone.gemfire.internal.cache;

/**
 * This is a dunit test for PartitionedRegion creation and Region API's
 * functionality. This test is performed for different region scopes - D_ACK and
 * D_NO_ACK for PartitionedRegion.
 */

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.TestCacheLoader;
import com.gemstone.gemfire.distributed.internal.ReplyException;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class PartitionedRegionAPIDUnitTest extends
		PartitionedRegionDUnitTestCase {

	public PartitionedRegionAPIDUnitTest(String name) {
		super(name);
	}

	Properties props = new Properties();

	VM vm0 = null;

	VM vm1 = null;

	VM vm2 = null;

	VM vm3 = null;

	final int putRange_1Start = 1;

	final int putRange_1End = 5;

	final int putRange_2Start = 6;

	final int putRange_2End = 10;

	final int putRange_3Start = 11;

	final int putRange_3End = 15;

	final int putRange_4Start = 16;

	final int putRange_4End = 20;

	final int removeRange_1Start = 2;

	final int removeRange_1End = 4;

	final int removeRange_2Start = 7;

	final int removeRange_2End = 9;

	// Create counters
	final int createRange_1Start = 21;

	final int createRange_1End = 25;

	final int createRange_2Start = 26;

	final int createRange_2End = 30;

	final int createRange_3Start = 31;

	final int createRange_3End = 35;

	final int createRange_4Start = 36;

	final int createRange_4End = 40;

	// Invalidate Counters
	final int invalidateRange_1Start = 41;

	final int invalidateRange_1End = 45;

	final int invalidateRange_2Start = 46;

	final int invalidateRange_2End = 50;

	final int invalidateRange_3Start = 51;

	final int invalidateRange_3End = 55;

	final int invalidateRange_4Start = 56;

	final int invalidateRange_4End = 60;

	// Used for size and isEmpty API Validation
	final int sizeRange_1Start = 61;

	final int sizeRange_1End = 65;

	final int sizeRange_2Start = 66;

	final int sizeRange_2End = 70;

	static final int totalNumBuckets = 5;

	/* SerializableRunnable object to create PR with scope = D_ACK */
	SerializableRunnable createPrRegionWithDS_DACK = new CacheSerializableRunnable(
			"createPrRegionWithDS") {

		public void run2() throws CacheException {
			Cache cache = getCache();
			AttributesFactory attr = new AttributesFactory();
			PartitionAttributesFactory paf = new PartitionAttributesFactory();
			paf.setTotalNumBuckets(totalNumBuckets);
			PartitionAttributes prAttr = paf.create();
			attr.setPartitionAttributes(prAttr);
			RegionAttributes regionAttribs = attr.create();
			cache.createRegion("PR1",
					regionAttribs);
		}
	};

	/*
	 * SerializableRunnable object to create PR with scope = D_ACK with only
	 * Accessor(no data store)
	 */

	SerializableRunnable createPrRegionOnlyAccessor_DACK = new CacheSerializableRunnable(
			"createPrRegionOnlyAccessor") {

		public void run2() throws CacheException {
			Cache cache = getCache();
			AttributesFactory attr = new AttributesFactory();
			PartitionAttributesFactory paf = new PartitionAttributesFactory();
			PartitionAttributes prAttr = paf.setLocalMaxMemory(0)
                          .setTotalNumBuckets(totalNumBuckets).create();
			attr.setPartitionAttributes(prAttr);
			RegionAttributes regionAttribs = attr.create();
			cache.createRegion("PR1",
					regionAttribs);
			LogWriterUtils.getLogWriter().info("Region created in VM1.");
		}
	};

  /**
   * Search the entires PartitionedRegion for the key, to validate that indeed
   * it doesn't exist
   * 
   * @returns true if it does exist
   * @param par
   * @param key
   */
  public static boolean searchForKey(PartitionedRegion par, Object key) {
    // Check to make super sure that the key doesn't exist ANYWHERE
    // TODO use keys() when it is properly implemented
    boolean foundIt = false;
    final int numBucks = par.getTotalNumberOfBuckets();
    for (int b = 0; b < numBucks; b++) {
      if (par.getBucketKeys(b).contains(key)) {
        foundIt = true;
        LogWriterUtils.getLogWriter().severe("Key " + key + " found in bucket " + b);
        break;
      }
    }
    if (!foundIt) {
      LogWriterUtils.getLogWriter().severe("Key " + key + " not found in any bucket");
    }
    return foundIt;
  }
  
	/**
	 * Test the Region operations after the PartitionedRegion has been destroyed
	 * 
	 * @param prName
	 */
	public void partitionedRegionTestAfterDestroyRegion(final String prName) {
		/*
		 * do some put(), create(), invalidate() operations for PR with accessor +
		 * Datastore and validate.
		 */
		vm0.invoke(new CacheSerializableRunnable(
				"doPutCreateInvalidateOperations1") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				String exceptionStr = "";
				Region pr = cache.getRegion(prName);
				if (pr == null) {
					fail("PR not created");
				}
				for (int i = putRange_1Start; i <= putRange_1End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put("" + i, "" + i);
				}

				//Create Operation
				for (int i = createRange_1Start; i <= createRange_1End; i++) {
					Object val = null;
					Object key = "" + i;
					if (i % 2 == 0) {
						val = "" + i;
					}
					pr.create(key, val);
				}

				for (int i = createRange_1Start; i <= createRange_1End; i++) {
					Object val = null;
					Object key = "" + i;
					if (i % 2 == 0) {
						val = "" + i;
					}
					final String expectedExceptions = EntryExistsException.class
							.getName();
					getCache().getLogger().info(
							"<ExpectedException action=add>"
									+ expectedExceptions
									+ "</ExpectedException>");
					exceptionStr = ReplyException.class.getName() + ":" + expectedExceptions;
					vm1.invoke(addExceptionTag1(exceptionStr));
					vm2.invoke(addExceptionTag1(exceptionStr));
					vm3.invoke(addExceptionTag1(exceptionStr));
					addExceptionTag1(exceptionStr);
					try {
						pr.create(key, val);
						fail("EntryExistsException is not thrown");
					} catch (EntryExistsException expected) {
						// getLogWriter().fine("EntryExistsException is properly thrown");
					}

					vm1.invoke(removeExceptionTag1(exceptionStr));
					vm2.invoke(removeExceptionTag1(exceptionStr));
					vm3.invoke(removeExceptionTag1(exceptionStr));
					removeExceptionTag1(exceptionStr);
					getCache().getLogger().info(
							"<ExpectedException action=remove>"
									+ expectedExceptions
									+ "</ExpectedException>");

				}

				// Invalidate Operations

				for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
					// Check that before creating an entry it throws
					// EntryNotFoundException
					final Object val = Integer.toString(i);
					final Object key = Integer.toString(i);
					final String entryNotFoundException = EntryNotFoundException.class
							.getName();
					getCache().getLogger().info(
							"<ExpectedException action=add>"
									+ entryNotFoundException
									+ "</ExpectedException>");
					exceptionStr = ReplyException.class.getName() + "||" + entryNotFoundException;
					vm1.invoke(addExceptionTag1(exceptionStr));
					vm2.invoke(addExceptionTag1(exceptionStr));
					vm3.invoke(addExceptionTag1(exceptionStr));
					addExceptionTag1(exceptionStr);
					try {
						pr.invalidate(key);
						fail("EntryNotFoundException is not thrown for key which does not exists in the system = "
								+ key);
					} catch (EntryNotFoundException expected) {
					}
					vm1.invoke(removeExceptionTag1(exceptionStr));
					vm2.invoke(removeExceptionTag1(exceptionStr));
					vm3.invoke(removeExceptionTag1(exceptionStr));
					removeExceptionTag1(exceptionStr);
					getCache().getLogger().info(
							"<ExpectedException action=remove>"
									+ entryNotFoundException
									+ "</ExpectedException>");
					pr.create(key, val);
					assertTrue("containsValueForKey key=" + key, pr
							.containsValueForKey(key));
					assertEquals(val, pr.get(key));
					pr.invalidate(key);
					assertFalse(pr.containsValueForKey(key));
					assertNull(pr.get(key));
				}
				for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
					final Object key = Integer.toString(i);
					pr.destroy(key);
				}

				final String entryNotFoundException = EntryNotFoundException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>"
								+ entryNotFoundException
								+ "</ExpectedException>");
				exceptionStr = ReplyException.class.getName()  + "||" + entryNotFoundException; 
				vm1.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
					// Check that after deleting an entry, invalidate for that entry
					// throws
					// EntryNotFoundException
					Object key = "" + i;
					try {
						pr.invalidate(key);
						fail("EntryNotFoundException is not thrown for key which does not exists in the system = "
								+ key);
					} catch (EntryNotFoundException expected) {
					}
				}
				vm1.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger().info(
						"<ExpectedException action=remove>"
								+ entryNotFoundException
								+ "</ExpectedException>");
				LogWriterUtils.getLogWriter().fine("Out of doPutOperations1");
				LogWriterUtils.getLogWriter().fine("All the puts done successfully for vm0.");
			}
		});

		/*
		 * do some put(), create(), invalidate() operations for PR with only
		 * accessor and validate.
		 */
		vm1.invoke(new CacheSerializableRunnable(
				"doPutCreateInvalidateOperations2") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				String exceptionStr = "";
				Region pr = cache.getRegion(prName);
				if (pr == null) {
					fail("PR not created");
				}
				for (int i = putRange_2Start; i <= putRange_2End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put("" + i, "" + i);
				}

				//Create Operation
				for (int i = createRange_2Start; i <= createRange_2End; i++) {
					Object val = null;
					Object key = "" + i;
					if (i % 2 == 0) {
						val = "" + i;
					}
					pr.create(key, val);
				}

				final String entryExistsException = EntryExistsException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + entryExistsException
								+ "</ExpectedException>");
				exceptionStr =  ReplyException.class.getName() + "||" + entryExistsException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = createRange_2Start; i <= createRange_2End; i++) {
					Object val = null;
					Object key = "" + i;
					if (i % 2 == 0) {
						val = "" + i;
					}
					try {
						pr.create(key, val);
						fail("EntryExistsException is not thrown");
					} catch (EntryExistsException expected) {
						// getLogWriter().fine("EntryExistsException is properly thrown");
					}
				}
				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger()
						.info(
								"<ExpectedException action=remove>"
										+ entryExistsException
										+ "</ExpectedException>");

				// Invalidate Operations
				final String entryNotFoundException = EntryNotFoundException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>"
								+ entryNotFoundException
								+ "</ExpectedException>");
				exceptionStr = ReplyException.class.getName() + "||" + entryNotFoundException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
					// Check that before creating an entry it throws
					// EntryNotFoundException
					final Object val = Integer.toString(i);
					final Object key = Integer.toString(i);

					try {
						pr.invalidate(key);
						fail("EntryNotFoundException is not thrown for key which does not exists in the system = "
								+ key);
					} catch (EntryNotFoundException expected) {
					}
					pr.create(key, val);
				}
				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger().info(
						"<ExpectedException action=remove>"
								+ entryNotFoundException
								+ "</ExpectedException>");

				for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
					// Check that before creating an entry it throws
					// EntryNotFoundException
					final Object val = Integer.toString(i);
					final Object key = Integer.toString(i);
					assertEquals(val, pr.get(key));
					assertTrue(pr.containsValueForKey(key));
					pr.invalidate(key);
				}

				for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
					final Object key = Integer.toString(i);
					Object shouldBeNull = pr.get(key);
					assertNull("Key " + key
							+ " should report val null, however it has "
							+ shouldBeNull, shouldBeNull);
					assertFalse("Key " + key
							+ " should report False for containsValueForKey",
							pr.containsValueForKey(key));
				}

				for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
					final Object key = Integer.toString(i);
					pr.destroy(key);
				}

				getCache().getLogger().info(
						"<ExpectedException action=add>"
								+ entryNotFoundException
								+ "</ExpectedException>");
				exceptionStr = ReplyException.class.getName() + "||" +
                                  entryNotFoundException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
					// Check that after deleting an entry, invalidate for that entry
					// throws
					// EntryNotFoundException
					final Object key = Integer.toString(i);
					try {
						pr.invalidate(key);
						fail("EntryNotFoundException is not thrown for key which does not exists in the system = "
								+ key);
					} catch (EntryNotFoundException expected) {
					}
				}
				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger().info(
						"<ExpectedException action=remove>"
								+ entryNotFoundException
								+ "</ExpectedException>");

				LogWriterUtils.getLogWriter().fine("Out of doPutOperations2");
				LogWriterUtils.getLogWriter().fine("All the puts done successfully for vm1.");
			}
		});
	}

	public void partitionedRegionTest(final String prName) {
		/*
		 * Do put(), create(), invalidate() operations through VM with PR having
		 * both Accessor and Datastore
		 */
		//String exceptionStr = "";
		vm0.invoke(new CacheSerializableRunnable(
				"doPutCreateInvalidateOperations1") {
			public void run2() throws CacheException {
				Cache cache = getCache();
				final Region pr = cache.getRegion(prName);
				if (pr == null) {
					fail(prName + " not created");
				}
				int size = 0;
//				if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
					size = pr.size();
					assertEquals("Size doesnt return expected value",
							0, size);
					assertEquals(
							"isEmpty doesnt return proper state of the PartitionedRegion",
							true, pr.isEmpty());
                                        assertEquals(0, pr.keySet().size());
                                        assertEquals(0, pr.keys().size());
//				}
				for (int i = putRange_1Start; i <= putRange_1End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put(Integer.toString(i), Integer.toString(i));
				}
//				if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
					size = pr.size();
					assertEquals("Size doesn't return expected value", putRange_1End, size);
					assertEquals("isEmpty doesnt return proper state of the PartitionedRegion", 
                                            false, pr.isEmpty());

                                        // Positive assertion of functionality in a distributed env.
                                        // For basic functional support (or lack of), please see PartitionedRegionSingleNodeOperationsJUnitTest 
                                        assertEquals(putRange_1End, pr.keySet().size());
                                        assertEquals(putRange_1End, pr.keys().size());
                                        Set ks = pr.keySet();
                                        Iterator ksI = ks.iterator(); 
                                        while (ksI.hasNext()) {
                                         try {
                                            ksI.remove();
                                            fail("Expected key set iterator to be read only");
                                          } catch (Exception expected) {}
                                          Object key = ksI.next();
                                          assertEquals(String.class, key.getClass());
                                          Integer.parseInt((String)key);
                                        }
                                        try {
                                          ksI.remove();
                                          fail("Expected key set iterator to be read only");
                                        } catch (Exception expected) {}
                                        assertFalse(ksI.hasNext());
                                        try {
                                          ksI.next();
                                          fail("Expected no such element exception");
                                        } catch (NoSuchElementException expected) {
                                          assertFalse(ksI.hasNext());
                                        }
//				}


				String exceptionStr = ReplyException.class.getName() + "||" + EntryNotFoundException.class.getName();
				vm1.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = putRange_1Start; i <= putRange_1End; i++) {
					// System.out.println("Putting entry for key = " + i);
					try {
						pr.destroy(Integer.toString(i));
					} catch (EntryNotFoundException enfe) {
						searchForKey((PartitionedRegion) pr, Integer
								.toString(i));
						throw enfe;
					}
				}
				vm1.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);

//				if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
					size = pr.size();
					assertEquals(
							"Size doesnt return expected value = 0 instead it returns"
									+ size, size, 0);
					assertEquals(
							"isEmpty doesnt return proper state of the PartitionedRegion",
							pr.isEmpty(), true);
//				}
				for (int i = putRange_1Start; i <= putRange_1End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put(Integer.toString(i), Integer.toString(i));
				}

				//createInvalidateChange
				for (int i = createRange_1Start; i <= createRange_1End; i++) {
					Object val = null;
					Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					pr.create(key, val);
				}

				final String expectedExceptions = EntryExistsException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + expectedExceptions
								+ "</ExpectedException>");
                                exceptionStr = ReplyException.class.getName() + "||" + expectedExceptions;
				vm1.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = createRange_1Start; i <= createRange_1End; i++) {
					Object val = null;
					Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					try {
						pr.create(key, val);
						fail("EntryExistsException is not thrown");
					} catch (EntryExistsException expected) {
						// cache.getLogger().fine("EntryExistsException is properly thrown");
					}
				}
				vm1.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger().info(
						"<ExpectedException action=remove>"
								+ expectedExceptions + "</ExpectedException>");
//				if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
					size = pr.size();
					assertEquals("Size doesnt return expected value", size, 10);
//				}
				LogWriterUtils.getLogWriter().fine(
						"All the puts done successfully for vm0.");
                                
                                
                                {
                                  PartitionedRegion ppr = (PartitionedRegion)pr;
                                  try {
                                    ppr.dumpAllBuckets(true);
                                  } catch (ReplyException re) {
                                    fail();
                                  }
                                }

			}
		});

		/*
		 * Do put(), create(), invalidate() operations through VM with PR having
		 * only Accessor(no data store)
		 */

		vm1.invoke(new CacheSerializableRunnable(
				"doPutCreateInvalidateOperations2") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				if (pr == null) {
					fail("PR not created");
				}

				for (int i = putRange_2Start; i <= putRange_2End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put(Integer.toString(i), Integer.toString(i));
				}

				//createInvalidateChange
				for (int i = createRange_2Start; i <= createRange_2End; i++) {
					Object val = null;
					Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					pr.create(key, val);
				}

				final String entryExistsException = EntryExistsException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + entryExistsException
								+ "</ExpectedException>");
				String exceptionStr = ReplyException.class.getName() + ":" + entryExistsException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);

				for (int i = createRange_2Start; i <= createRange_2End; i++) {
					Object val = null;
					Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					try {
						pr.create(key, val);
						fail("EntryExistsException is not thrown");
					} catch (EntryExistsException expected) {
						// cache.getLogger().fine("EntryExistsException is properly thrown");
					}
				}

				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger()
						.info(
								"<ExpectedException action=remove>"
										+ entryExistsException
										+ "</ExpectedException>");
				cache.getLogger().fine(
						"All the puts done successfully for vm1.");

			}
		});

		/*
		 * Do destroy() operations through VM with PR having only Accessor(no data
		 * store). It also verifies that EntryNotFoundException is thrown if the
		 * entry is already destroyed.
		 */

		vm1.invoke(new CacheSerializableRunnable("doRemoveOperations1") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				if (pr == null) {
					fail("PR not created");
				}
				for (int i = removeRange_1Start; i <= removeRange_1End; i++) {
					// System.out.println("destroying entry for key = " + i);
					final String key = Integer.toString(i);
					try {
						pr.destroy(key);
					} catch (EntryNotFoundException enfe) {
						searchForKey((PartitionedRegion) pr, key);
						throw enfe;
					}

				}

				final String entryNotFoundException = EntryNotFoundException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + entryNotFoundException
								+ "</ExpectedException>");
				String exceptionStr = ReplyException.class.getName() + "||" + entryNotFoundException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (int i = removeRange_1Start; i <= removeRange_1End; i++) {
					final String key = Integer.toString(i);
					try {
						pr.destroy(key);
						fail("EntryNotFoundException is not thrown in destroy operation for key = "
								+ i);
					} catch (EntryNotFoundException expected) {
					}
				}

				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger()
						.info(
								"<ExpectedException action=remove>"
										+ entryNotFoundException
										+ "</ExpectedException>");
				LogWriterUtils.getLogWriter()
						.fine("All the remove done successfully for vm0.");
			}
		});

		/*
		 * Do more put(), create(), invalidate() operations through VM with PR
		 * having Accessor + data store
		 */
		vm2.invoke(new CacheSerializableRunnable(
				"doPutCreateInvalidateOperations3") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				assertNotNull("PR not created", pr);

				for (int i = putRange_3Start; i <= putRange_3End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put(Integer.toString(i), Integer.toString(i));
				}

				for (int i = createRange_3Start; i <= createRange_3End; i++) {
					Object val = null;
					Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					pr.create(key, val);
				}
				final String entryExistsException = EntryExistsException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + entryExistsException
								+ "</ExpectedException>");
				String exceptionStr = ReplyException.class.getName() + "||" + entryExistsException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm1.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);

				for (int i = createRange_3Start; i <= createRange_3End; i++) {
					Object val = null;
					Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					try {
						pr.create(key, val);
						fail("EntryExistsException is not thrown");
					} catch (EntryExistsException expected) {
						// getLogWriter().fine("EntryExistsException is properly thrown");
					}
				}

				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm1.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger()
						.info(
								"<ExpectedException action=remove>"
										+ entryExistsException
										+ "</ExpectedException>");

			}
		});

		/*
		 * Do more remove() operations through VM with PR having Accessor + data
		 * store
		 */

		vm2.invoke(new CacheSerializableRunnable("doRemoveOperations2") {

			public void run2() throws CacheException {
				int i = 0;
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				assertNotNull("PR not created", pr);

				if (pr == null) {
					fail("PR not created");
				}
                                String key;
				for (i = removeRange_2Start; i <= removeRange_2End; i++) {
				  // System.out.println("destroying entry for key = " + i);
                                  key = Integer.toString(i);
				  try {
				    pr.destroy(key);
				  } catch (EntryNotFoundException enfe) {
				    searchForKey((PartitionedRegion) pr, key);
				    throw enfe;
				  }
				}

				final String entryNotFound = EntryNotFoundException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + entryNotFound
								+ "</ExpectedException>");
				String exceptionStr = ReplyException.class.getName() + "||" + entryNotFound;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm1.invoke(addExceptionTag1(exceptionStr));
				vm3.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);
				for (i = removeRange_2Start; i <= removeRange_2End; i++) {
					// System.out.println("destroying entry for key = " + i);
					try {
						pr.destroy(Integer.toString(i));
						fail("EntryNotFoundException is not thrown in destroy operation for key = "
								+ (Integer.toString(i)));
					} catch (EntryNotFoundException expected) {
					}
				}
				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm1.invoke(removeExceptionTag1(exceptionStr));
				vm3.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);
				getCache().getLogger().info(
						"<ExpectedException action=remove>" + entryNotFound
								+ "</ExpectedException>");
			}
		});

		/*
		 * Do more put() operations through VM with PR having only Accessor
		 */

		vm3.invoke(new CacheSerializableRunnable(
				"doPutCreateInvalidateOperations4") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				assertNotNull("PR not created", pr);

				for (int i = putRange_4Start; i <= putRange_4End; i++) {
					// System.out.println("Putting entry for key = " + i);
					pr.put(Integer.toString(i), Integer.toString(i));
				}
				for (int i = createRange_4Start; i <= createRange_4End; i++) {
					Object val = null;
					final Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					pr.create(key, val);
				}

				final String entryExistsException = EntryExistsException.class
						.getName();
				getCache().getLogger().info(
						"<ExpectedException action=add>" + entryExistsException
								+ "</ExpectedException>");
				String exceptionStr = ReplyException.class.getName() + "||" + entryExistsException;
				vm0.invoke(addExceptionTag1(exceptionStr));
				vm1.invoke(addExceptionTag1(exceptionStr));
				vm2.invoke(addExceptionTag1(exceptionStr));
				addExceptionTag1(exceptionStr);

				for (int i = createRange_4Start; i <= createRange_4End; i++) {
					Object val = null;
					final Object key = Integer.toString(i);
					if (i % 2 == 0) {
						val = Integer.toString(i);
					}
					try {
						pr.create(key, val);
						fail("EntryExistsException is not thrown");
					} catch (EntryExistsException expected) {
						// getLogWriter().fine("EntryExistsException is properly thrown");
					}
				}

				vm0.invoke(removeExceptionTag1(exceptionStr));
				vm1.invoke(removeExceptionTag1(exceptionStr));
				vm2.invoke(removeExceptionTag1(exceptionStr));
				removeExceptionTag1(exceptionStr);

				getCache().getLogger()
						.info(
								"<ExpectedException action=remove>"
										+ entryExistsException
										+ "</ExpectedException>");

			}
		});

		/*
		 * validate the data in PartionedRegion at different VM's
		 *  
		 */
		CacheSerializableRunnable validateRegionAPIs = new CacheSerializableRunnable(
				"validateInserts") {

			public void run2() {
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				assertNotNull("PR not created", pr);

				// Validation with get() operation.
				for (int i = putRange_1Start; i <= putRange_4End; i++) {
					Object val = pr.get(Integer.toString(i));
					if ((i >= removeRange_1Start && i <= removeRange_1End)
							|| (i >= removeRange_2Start && i <= removeRange_2End)) {
						assertNull("Remove validation failed for key " + i, val);
					} else {
						assertNotNull("put() not done for key " + i, val);
					}
				}
				// validation with containsKey() operation.
				for (int i = putRange_1Start; i <= putRange_4End; i++) {
					boolean conKey = pr.containsKey(Integer.toString(i));
					if ((i >= removeRange_1Start && i <= removeRange_1End)
							|| (i >= removeRange_2Start && i <= removeRange_2End)) {
						assertFalse(
								"containsKey() remove validation failed for key = "
										+ i, conKey);
					} else {
						assertTrue("containsKey() Validation failed for key = "
								+ i, conKey);
					}
					LogWriterUtils.getLogWriter().fine(
							"containsKey() Validated entry for key = " + i);
				}

				// validation with containsValueForKey() operation
				for (int i = putRange_1Start; i <= putRange_4End; i++) {
					boolean conKey = pr
							.containsValueForKey(Integer.toString(i));
					if ((i >= removeRange_1Start && i <= removeRange_1End)
							|| (i >= removeRange_2Start && i <= removeRange_2End)) {
						assertFalse(
								"containsValueForKey() remove validation failed for key = "
										+ i, conKey);
					} else {
						assertTrue(
								"containsValueForKey() Validation failed for key = "
										+ i, conKey);
					}
					LogWriterUtils.getLogWriter().fine(
							"containsValueForKey() Validated entry for key = "
									+ i);
				}
			}
		};

		// validate the data from all the VM's
		vm0.invoke(validateRegionAPIs);
		vm1.invoke(validateRegionAPIs);
		vm2.invoke(validateRegionAPIs);
		vm3.invoke(validateRegionAPIs);

		/*
		 * destroy the Region.
		 */
		vm0.invoke(new CacheSerializableRunnable("destroyRegionOp") {

			public void run2() {
				Cache cache = getCache();
				Region pr = cache.getRegion(prName);
				assertNotNull("Region already destroyed.", pr);
				pr.destroyRegion();
				assertTrue("Region isDestroyed false", pr.isDestroyed());
				assertNull("Region not destroyed.", cache.getRegion(prName));
			}
		});

		/*
		 * validate the data after the region.destroy() operation.
		 */
		CacheSerializableRunnable validateAfterRegionDestroy = new CacheSerializableRunnable(
				"validateInsertsAfterRegionDestroy") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				Region pr = null;
				pr = cache.getRegion(prName);
				assertNull("Region not destroyed.", pr);
				Region rootRegion = cache.getRegion(Region.SEPARATOR
						+ PartitionedRegionHelper.PR_ROOT_REGION_NAME);
				// Verify allPartitionedRegion.
//				Region allPrs = rootRegion
//						.getSubregion(PartitionedRegionHelper.PARTITIONED_REGION_CONFIG_NAME);
				Object configObj = rootRegion.get(prName.substring(1));
				if (configObj != null) {
					fail("PRConfig found in allPartitionedRegion Metadata for this PR.");
				}
				// Verify b2n region.
//				Region b2nReg = rootRegion
//						.getSubregion(PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX);
//				if (b2nReg != null) {
//					fail("PRConfig found in allPartitionedRegion Metadata for this PR.");
//				}
				// Verify bucket Regions.
				Set subreg = rootRegion.subregions(false);
				for (java.util.Iterator itr = subreg.iterator(); itr.hasNext();) {
					Region reg = (Region) itr.next();
					String name = reg.getName();
					if ((name
							.indexOf(PartitionedRegionHelper.BUCKET_REGION_PREFIX)) != -1) {
						fail("Bucket exists. Bucket = " + name);
					}
				}
				// verify prIdToPr Map.
				boolean con = PartitionedRegion.prIdToPR.containsKey("PR1");
				if (con == true) {
					fail("prIdToPR contains pr reference ");
				}
			}
		};

		// validateAfterRegionDestory from all VM's

		vm0.invoke(validateAfterRegionDestroy);
		vm1.invoke(validateAfterRegionDestroy);
		vm2.invoke(validateAfterRegionDestroy);
		vm3.invoke(validateAfterRegionDestroy);
	}

      
  /**
   * Do putIfAbsent(), replace(Object, Object),
   * replace(Object, Object, Object), remove(Object, Object) operations
   * through VM with PR having
   * both Accessor and Datastore
   */
  public void partitionedRegionConcurrentMapTest(final String prName) {
    //String exceptionStr = "";
    vm0.invoke(new CacheSerializableRunnable("doConcurrentMapOperations") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        final PartitionedRegion pr = (PartitionedRegion)cache.getRegion(prName);
        assertNotNull(prName + " not created", pr);
               
        // test successful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i),
                                            Integer.toString(i));
          assertNull("Expected null, but got " + putResult + "for key " + i,
                     putResult);
        }
        int size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                    pr.isEmpty());
        
        // test unsuccessful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i),
                                            Integer.toString(i + 1));
          assertEquals("for i=" + i, Integer.toString(i), putResult);
          assertEquals("for i=" + i, Integer.toString(i), pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                    pr.isEmpty());
               
        // test successful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
         boolean replaceSucceeded = pr.replace(Integer.toString(i),
                                               Integer.toString(i),
                                               "replaced" + i);
          assertTrue("for i=" + i, replaceSucceeded);
          assertEquals("for i=" + i, "replaced" + i, pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                   pr.isEmpty());
               
        // test unsuccessful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
         boolean replaceSucceeded = pr.replace(Integer.toString(i),
                                               Integer.toString(i), // wrong expected old value
                                               "not" + i);
         assertFalse("for i=" + i, replaceSucceeded);
         assertEquals("for i=" + i,
                      i <= putRange_1End ? "replaced" + i : null,
                      pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                   pr.isEmpty());
                                    
        // test successful replace(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i),
                                            "twice replaced" + i);
          assertEquals("for i=" + i, "replaced" + i, replaceResult);
          assertEquals("for i=" + i,
                       "twice replaced" + i,
                       pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                    pr.isEmpty());
                                    
        // test unsuccessful replace(key, value)
        for (int i = putRange_2Start; i <= putRange_2End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i),
                                           "thrice replaced" + i);
          assertNull("for i=" + i, replaceResult);
          assertNull("for i=" + i, pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                    pr.isEmpty());
                                    
        // test unsuccessful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i),
                                           Integer.toString(-i));
          assertFalse("for i=" + i, removeResult);
          assertEquals("for i=" + i,
                       i <= putRange_1End ? "twice replaced" + i : null,
                       pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", putRange_1End, size);
        assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                    pr.isEmpty());

        // test successful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i),
                                           "twice replaced" + i);
          assertTrue("for i=" + i, removeResult);
          assertEquals("for i=" + i, null, pr.get(Integer.toString(i)));
        }
        size = pr.size();
        assertEquals("Size doesn't return expected value", 0, size);
        assertTrue("isEmpty doesnt return proper state of the PartitionedRegion", 
                 pr.isEmpty());
        }
    });
    
    
    /*
     * destroy the Region.
     */
    vm0.invoke(new CacheSerializableRunnable("destroyRegionOp") {
               
       public void run2() {
         Cache cache = getCache();
         Region pr = cache.getRegion(prName);
         assertNotNull("Region already destroyed.", pr);
         pr.destroyRegion();
         assertTrue("Region isDestroyed false", pr.isDestroyed());
         assertNull("Region not destroyed.", cache.getRegion(prName));
       }
     });
  }
      
      
      
	/**
	 * This is a PartitionedRegion test for scope = D_ACK. 4 VMs are used to
	 * create the PR with and without(Only Accessor) the DataStore.
	 */
	public void testPartitionedRegionOperationsScopeDistAck() throws Exception {
	  Host host = Host.getHost(0);
	  // create the VM(0 - 4)
	  vm0 = host.getVM(0);
	  vm1 = host.getVM(1);
	  vm2 = host.getVM(2);
	  vm3 = host.getVM(3);
          final VM accessor = vm3;
	  // Create PR;s in different VM's
	  vm0.invoke(createPrRegionWithDS_DACK);
	  vm1.invoke(createPrRegionOnlyAccessor_DACK);
	  vm2.invoke(createPrRegionWithDS_DACK);
	  accessor.invoke(createPrRegionOnlyAccessor_DACK);
//	  final String expectedExceptions = ReplyException.class.getName();
	  //addExceptionTag(expectedExceptions);
	  partitionedRegionTest("/PR1");
	  //removeExceptionTag(expectedExceptions);
	  // Again create the Region with same name
	  vm0.invoke(createPrRegionWithDS_DACK);
	  vm1.invoke(createPrRegionOnlyAccessor_DACK);
	  vm2.invoke(createPrRegionWithDS_DACK);
	  accessor.invoke(createPrRegionOnlyAccessor_DACK);

	  //addExceptionTag(expectedExceptions);
	  partitionedRegionTestAfterDestroyRegion("/PR1");
	  //addExceptionTag(expectedExceptions); 
	  /*
	   * destroy the Region.
	   */
	  destroyTheRegion("/PR1");
	}
      
  /**
   * This is a PartitionedRegion test for the ConcurrentMap operations
   * for scope = D_ACK. 4 VMs are used to
   * create the PR with and without(Only Accessor) the DataStore.
   */
  public void testPartitionedRegionConcurrentOperations() throws Exception {
    Host host = Host.getHost(0);
    // create the VM(0 - 4)
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    final VM accessor = vm3;
    // Create PR;s in different VM's
    vm0.invoke(createPrRegionWithDS_DACK);
    vm1.invoke(createPrRegionOnlyAccessor_DACK);
    vm2.invoke(createPrRegionWithDS_DACK);
    accessor.invoke(createPrRegionOnlyAccessor_DACK);
//    final String expectedExceptions = ReplyException.class.getName();
    partitionedRegionConcurrentMapTest("/PR1");
  }
      

	/**
   * Test the PartitionedRegion operations when the Scope is set to
   * {@link AttributesFactory#setEarlyAck(boolean)}
   * 
   * @throws Exception
   */
  public void testPartitionedRegionsOperationsScopeDistEarlyAck()
  		throws Exception {
  	final String rName = getUniqueName();
  	Host host = Host.getHost(0);
  	vm0 = host.getVM(0);
  	vm1 = host.getVM(1);
  	vm2 = host.getVM(2);
  	vm3 = host.getVM(3);
  	CacheSerializableRunnable create = new CacheSerializableRunnable(
  			"createPRWithEarlyAck") {
  		public void run2() throws CacheException {
  			Cache cache = getCache();
  			AttributesFactory attr = new AttributesFactory();
  			attr.setEarlyAck(true);
  			attr.setPartitionAttributes(new PartitionAttributesFactory()
  					.create());
  			RegionAttributes regionAttribs = attr.create();
  			Region partitionedregion = cache.createRegion(rName,
  					regionAttribs);
  			assertNotNull(partitionedregion);
  			assertNotNull(cache.getRegion(rName));
  		}
  	};
  	CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
  			"createPRAccessorWithEarlyAck") {
  		public void run2() throws CacheException {
  			Cache cache = getCache();
  			AttributesFactory attr = new AttributesFactory();
  			attr.setEarlyAck(true);
  			PartitionAttributes prAttr = new PartitionAttributesFactory()
  					.setLocalMaxMemory(0).create();
  			attr.setPartitionAttributes(prAttr);
  			RegionAttributes regionAttribs = attr.create();
  			Region partitionedregion = cache.createRegion(rName,
  					regionAttribs);
  			assertNotNull(partitionedregion);
  			assertNotNull(cache.getRegion(rName));
  		}
  	};
  
  	vm0.invoke(create);
  	vm1.invoke(createAccessor);
  	vm2.invoke(create);
  	vm3.invoke(createAccessor);
//  	final String expectedExceptions = ReplyException.class.getName();
  	//addExceptionTag(expectedExceptions);
  	partitionedRegionTest(rName); // Assumed to destroy the region
  	//removeExceptionTag(expectedExceptions);
  	// Again create the Region with same name
  	vm0.invoke(create);
  	vm1.invoke(createAccessor);
  	vm2.invoke(create);
  	vm3.invoke(createAccessor);
  	//addExceptionTag(expectedExceptions);
  	partitionedRegionTestAfterDestroyRegion(rName);
  	//removeExceptionTag(expectedExceptions);
  	destroyTheRegion(rName);
  }

  /**
   * Verify that localMaxMemory is set correctly when using attributes
   * 
   * @throws Exception
   */
  public void testBug36685() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    CacheSerializableRunnable create = new CacheSerializableRunnable(
        "createPR") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setDataPolicy(DataPolicy.PARTITION);
        RegionAttributes regionAttribs = attr.create();
        Region partitionedregion = cache.createRegion(rName, regionAttribs);
        assertNotNull(partitionedregion);
        assertNotNull(cache.getRegion(rName));
        PartitionAttributes p = regionAttribs.getPartitionAttributes();
        int maxMem = p.getLocalMaxMemory();
        assertTrue("LocalMaxMemory is zero", maxMem != 0);
      }
    };
    
    vm0.invoke(create);
    destroyTheRegion(rName);
  }


	public void destroyTheRegion(final String name) {
		/*
		 * destroy the Region.
		 */
		vm0.invoke(new CacheSerializableRunnable("destroyRegionOp") {

			public void run2() throws CacheException {
				Cache cache = getCache();
				Region pr = cache.getRegion(name);
				if (pr == null) {
					fail(name + " not created");
				}
				pr.destroyRegion();
			}
		});
	}

	protected CacheSerializableRunnable addExceptionTag1(final String expectedException) {
	  CacheSerializableRunnable addExceptionTag = new CacheSerializableRunnable(
	  "addExceptionTag") {
	    public void run2()
	    {
	      getCache().getLogger().info(
	          "<ExpectedException action=add>" + expectedException
	          + "</ExpectedException>");
	    }
	  };
	  
	  return addExceptionTag;
	}

	protected CacheSerializableRunnable removeExceptionTag1(final String expectedException) {
	  CacheSerializableRunnable removeExceptionTag = new CacheSerializableRunnable(
	  "removeExceptionTag") {
	    public void run2() throws CacheException {
	      getCache().getLogger().info(
	          "<ExpectedException action=remove>" + expectedException
	          + "</ExpectedException>");
	    }
	  };
	  return removeExceptionTag;
	}

        
	public void testCacherLoaderHelper() throws Exception 
	{
	  final String rName = getUniqueName();
	  Host host = Host.getHost(0);
	  VM vm2 = host.getVM(2);
	  VM vm3 = host.getVM(3);
	  
	  final int localMaxMemory = 10;
          final String key1 = "key1";
          final String arg = "loaderArg";
	  CacheSerializableRunnable createLoaderPR = new CacheSerializableRunnable("createLoaderPR") {
	    public void run2() throws CacheException {
              getCache();
              
              CacheLoader cl = new TestCacheLoader() {
                public Object load2(LoaderHelper helper) throws CacheLoaderException
                {
                  assertNotNull(helper);
                  assertEquals(key1, helper.getKey());
                  assertEquals(rName, helper.getRegion().getName());
                  assertEquals(arg, helper.getArgument());
                  return helper.getArgument();
                }
              };
              
	      PartitionedRegion pr = (PartitionedRegion) new RegionFactory()
                      .setCacheLoader(cl)
        	      .setPartitionAttributes(
        	          new PartitionAttributesFactory()
                	          .setRedundantCopies(1)
                	          .setLocalMaxMemory(localMaxMemory)
                	          .create())
        	      .create(rName);
              assertSame(cl, pr.getDataStore().getCacheLoader());
	    }
	  };
	  vm2.invoke(createLoaderPR);
	  vm3.invoke(createLoaderPR);

          // create a "pure" accessor, no data storage
          getCache();
	  Region pr = new RegionFactory()
	  .setPartitionAttributes(
	      new PartitionAttributesFactory()
	      .setRedundantCopies(1)
	      .setLocalMaxMemory(0)
	      .create())
          .create(rName);
	  
	  assertEquals(arg, pr.get(key1, arg));
	}
}
