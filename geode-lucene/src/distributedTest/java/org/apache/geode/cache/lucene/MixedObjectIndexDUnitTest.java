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
package org.apache.geode.cache.lucene;


import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.FloatRangeQueryProvider;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.IntRangeQueryProvider;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class MixedObjectIndexDUnitTest extends LuceneQueriesAccessorBase {

  protected RegionTestableType[] getPartitionRegionTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION,
        RegionTestableType.PARTITION_PERSISTENT, RegionTestableType.PARTITION_REDUNDANT,
        RegionTestableType.PARTITION_REDUNDANT_PERSISTENT};

  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    String filter = (String) result.get(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER);
    filter +=
        ";org.apache.geode.cache.lucene.MixedObjectIndexDUnitTest*;org.apache.geode.cache.lucene.test.LuceneTestUtilities*";
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER, filter);
    return result;
  }


  @Test
  @Parameters(method = "getPartitionRegionTypes")
  public void luceneCanIndexFieldsWithSameNameButInDifferentObjects(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = getSerializableRunnableIFCreateIndexOnFieldText();

    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));

    accessor.invoke(() -> initDataStore(createIndex, regionTestType));

    accessor.invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
      IntStream.range(NUM_BUCKETS, (2 * NUM_BUCKETS))
          .forEach(i -> region.put(i, new TestObjectWithSameFieldName("hello world 2")));
    });

    waitForFlushBeforeExecuteTextSearch(accessor, 60000);

    accessor.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      LuceneQuery luceneQuery = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, "world", "text");
      List resultList = luceneQuery.findResults();
      int objectType_1_count = 0;
      int objectType_2_count = 0;
      for (Object luceneResultStruct : resultList) {
        if (((LuceneResultStruct) luceneResultStruct).getValue() instanceof TestObject) {
          objectType_1_count++;
        } else if (((LuceneResultStruct) luceneResultStruct)
            .getValue() instanceof TestObjectWithSameFieldName) {
          objectType_2_count++;
        }
      }
      assertEquals(NUM_BUCKETS, objectType_1_count);
      assertEquals(NUM_BUCKETS, objectType_2_count);
    });
  }

  @Test
  @Parameters(method = "getPartitionRegionTypes")
  public void luceneMustIndexFieldsWithMixedObjects(RegionTestableType regionTestableType) {
    SerializableRunnableIF createIndexOnTextAndDataField =
        getSerializableRunnableIFCreateIndexOnFieldData();

    dataStore1.invoke(() -> initDataStore(createIndexOnTextAndDataField, regionTestableType));
    dataStore2.invoke(() -> initDataStore(createIndexOnTextAndDataField, regionTestableType));

    accessor.invoke(() -> initDataStore(createIndexOnTextAndDataField, regionTestableType));

    accessor.invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
      IntStream.range(NUM_BUCKETS, 2 * NUM_BUCKETS)
          .forEach(i -> region.put(i, new TestObjectWithSameFieldName("hello world")));
      IntStream.range(2 * NUM_BUCKETS, 3 * NUM_BUCKETS)
          .forEach(i -> region.put(i, new TestObjectWithNoCommonField("hello world")));
    });


    waitForFlushBeforeExecuteTextSearch(accessor, 60000);

    accessor.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      LuceneQuery luceneQueryForTextField = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, "world", "text");
      List luceneResults = luceneQueryForTextField.findResults();
      validateObjectResultCounts(luceneResults, TestObject.class, NUM_BUCKETS,
          TestObjectWithSameFieldName.class, NUM_BUCKETS, TestObjectWithNoCommonField.class, 0);

      luceneQueryForTextField = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, "world", "data");
      luceneResults = luceneQueryForTextField.findResults();
      validateObjectResultCounts(luceneResults, TestObject.class, 0,
          TestObjectWithSameFieldName.class, 0, TestObjectWithNoCommonField.class, NUM_BUCKETS);
    });
  }



  @Test
  @Parameters(method = "getPartitionRegionTypes")
  public void luceneMustIndexFieldsWithTheSameNameInARegionWithMixedObjects(
      RegionTestableType regionTestableType) {
    SerializableRunnableIF createIndexOnTextField =
        getSerializableRunnableIFCreateIndexOnFieldText();

    dataStore1.invoke(() -> initDataStore(createIndexOnTextField, regionTestableType));
    dataStore2.invoke(() -> initDataStore(createIndexOnTextField, regionTestableType));

    accessor.invoke(() -> initDataStore(createIndexOnTextField, regionTestableType));

    accessor.invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
      IntStream.range(NUM_BUCKETS, 2 * NUM_BUCKETS)
          .forEach(i -> region.put(i, new TestObjectWithSameFieldName("hello world")));
      IntStream.range(2 * NUM_BUCKETS, 3 * NUM_BUCKETS)
          .forEach(i -> region.put(i, new TestObjectWithNoCommonField("hello world")));
    });

    waitForFlushBeforeExecuteTextSearch(accessor, 60000);

    accessor.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      LuceneQuery luceneQueryForTextField = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, "world", "text");
      List luceneResults = luceneQueryForTextField.findResults();
      validateObjectResultCounts(luceneResults, TestObject.class, NUM_BUCKETS,
          TestObjectWithSameFieldName.class, NUM_BUCKETS, TestObjectWithNoCommonField.class, 0);
    });

  }

  @Test
  @Parameters(method = "getPartitionRegionTypes")
  public void luceneMustIndexFieldsWithTheSameNameDifferentDataTypeInARegionWithMixedObjects(
      RegionTestableType regionTestableType) {
    SerializableRunnableIF createIndexOnTextField =
        getSerializableRunnableIFCreateIndexOnFieldText();

    dataStore1.invoke(() -> initDataStore(createIndexOnTextField, regionTestableType));
    dataStore2.invoke(() -> initDataStore(createIndexOnTextField, regionTestableType));

    accessor.invoke(() -> initDataStore(createIndexOnTextField, regionTestableType));

    accessor.invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      IntStream.range(2 * NUM_BUCKETS, 3 * NUM_BUCKETS).forEach(i -> region.put(i,
          new TestObjectSameFieldNameButDifferentDataTypeInteger(1000)));
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
      IntStream.range(NUM_BUCKETS, 2 * NUM_BUCKETS).forEach(i -> region.put(i,
          new TestObjectSameFieldNameButDifferentDataTypeFloat(999.1f)));
    });

    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));

    accessor.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());

      LuceneQuery luceneQueryForTextField = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, "world", "text");

      List luceneResults = luceneQueryForTextField.findResults();
      validateObjectResultCounts(luceneResults, TestObject.class, NUM_BUCKETS,
          TestObjectSameFieldNameButDifferentDataTypeFloat.class, 0,
          TestObjectSameFieldNameButDifferentDataTypeInteger.class, 0);

      FloatRangeQueryProvider floatRangeQueryProvider =
          new FloatRangeQueryProvider("text", 999.0f, 999.2f);
      luceneQueryForTextField = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, floatRangeQueryProvider);

      luceneResults = luceneQueryForTextField.findResults();
      validateObjectResultCounts(luceneResults, TestObject.class, 0,
          TestObjectSameFieldNameButDifferentDataTypeFloat.class, NUM_BUCKETS,
          TestObjectSameFieldNameButDifferentDataTypeInteger.class, 0);

      IntRangeQueryProvider intRangeQueryProvider = new IntRangeQueryProvider("text", 1000, 1000);
      luceneQueryForTextField = luceneService.createLuceneQueryFactory().setLimit(100)
          .create(INDEX_NAME, REGION_NAME, intRangeQueryProvider);

      luceneResults = luceneQueryForTextField.findResults();
      validateObjectResultCounts(luceneResults, TestObject.class, 0,
          TestObjectSameFieldNameButDifferentDataTypeFloat.class, 0,
          TestObjectSameFieldNameButDifferentDataTypeInteger.class, NUM_BUCKETS);
    });
  }

  private void validateObjectResultCounts(List luceneResults, Class objectType_1,
      int expectedObjectType_1_count, Class objectType_2, int expectedObjectType_2_count,
      Class objectType_3, int expectedObjectType_3_count) {
    int actualObjectType_1_count = 0;
    int actualObjectType_2_count = 0;
    int actualObjectType_3_count = 0;

    for (Object luceneResult : luceneResults) {
      Object resultValue = ((LuceneResultStruct) luceneResult).getValue();
      if (objectType_1.isInstance(resultValue)) {
        actualObjectType_1_count++;
      } else if (objectType_2.isInstance(resultValue)) {
        actualObjectType_2_count++;
      } else if (objectType_3.isInstance(resultValue)) {
        actualObjectType_3_count++;
      }
    }

    assertEquals(expectedObjectType_1_count, actualObjectType_1_count);
    assertEquals(expectedObjectType_2_count, actualObjectType_2_count);
    assertEquals(expectedObjectType_3_count, actualObjectType_3_count);
  }

  private SerializableRunnableIF getSerializableRunnableIFCreateIndexOnFieldText() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };
  }

  private SerializableRunnableIF getSerializableRunnableIFCreateIndexOnFieldData() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("data", "text").create(INDEX_NAME, REGION_NAME);
    };
  }



  protected static class TestObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String text;

    public TestObject(String text) {
      this.text = text;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MixedObjectIndexDUnitTest.TestObject other = (MixedObjectIndexDUnitTest.TestObject) obj;
      if (text == null) {
        return other.text == null;
      } else
        return text.equals(other.text);
    }

    @Override
    public String toString() {
      return "TestObject[" + text + "]";
    }


  }

  protected static class TestObjectSameFieldNameButDifferentDataTypeFloat implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Float text;

    public TestObjectSameFieldNameButDifferentDataTypeFloat(Float text) {
      this.text = text;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestObjectSameFieldNameButDifferentDataTypeFloat other =
          (TestObjectSameFieldNameButDifferentDataTypeFloat) obj;
      if (text == null) {
        return other.text == null;
      } else
        return text.equals(other.text);
    }

    @Override
    public String toString() {
      return "TestObjectSameFieldNameButDifferentDataTypeFloat[" + text + "]";
    }
  }

  protected static class TestObjectSameFieldNameButDifferentDataTypeInteger
      implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Integer text;

    public TestObjectSameFieldNameButDifferentDataTypeInteger(Integer text) {
      this.text = text;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MixedObjectIndexDUnitTest.TestObjectSameFieldNameButDifferentDataTypeInteger other =
          (MixedObjectIndexDUnitTest.TestObjectSameFieldNameButDifferentDataTypeInteger) obj;
      if (text == null) {
        return other.text == null;
      } else
        return text.equals(other.text);
    }

    @Override
    public String toString() {
      return "TestObjectSameFieldNameButDifferentDataTypeInteger[" + text + "]";
    }
  }

  protected static class TestObjectWithSameFieldName implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String text;

    public TestObjectWithSameFieldName(String text) {
      this.text = text;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestObjectWithSameFieldName other = (TestObjectWithSameFieldName) obj;
      if (text == null) {
        return other.text == null;
      } else
        return text.equals(other.text);
    }

    @Override
    public String toString() {
      return "TestObjectWithSameFieldName[" + text + "]";
    }


  }

  protected static class TestObjectWithNoCommonField implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String data;

    public TestObjectWithNoCommonField(String data) {
      this.data = data;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((data == null) ? 0 : data.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestObjectWithNoCommonField other = (TestObjectWithNoCommonField) obj;
      if (data == null) {
        return other.data == null;
      } else
        return data.equals(other.data);
    }

    @Override
    public String toString() {
      return "TestObjectWithNoCommonField[" + data + "]";
    }


  }
}
