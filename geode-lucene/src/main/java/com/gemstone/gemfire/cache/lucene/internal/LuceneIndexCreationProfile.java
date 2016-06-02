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
package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.CacheServiceProfile;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import org.apache.lucene.analysis.Analyzer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class LuceneIndexCreationProfile implements CacheServiceProfile, DataSerializable {

  private String indexName;

  private String[] fieldNames;

  private Class<? extends Analyzer> analyzerClass;

  private Map<String, Class<? extends Analyzer>> fieldAnalyzers;

  /* Used by DataSerializer */
  public LuceneIndexCreationProfile() {}

  public LuceneIndexCreationProfile(String indexName, String[] fieldNames, Analyzer analyzer,
      Map<String, Analyzer> fieldAnalyzers) {
    this.indexName = indexName;
    this.fieldNames = fieldNames;
    this.analyzerClass = analyzer.getClass();
    initializeFieldAnalyzers(fieldAnalyzers);
  }

  public String getIndexName() {
    return this.indexName;
  }

  public String[] getFieldNames() {
    return this.fieldNames;
  }

  public Class<? extends Analyzer> getAnalyzerClass() {
    return this.analyzerClass;
  }

  public Map<String, Class<? extends Analyzer>> getFieldAnalyzers() {
    return this.fieldAnalyzers;
  }

  protected void initializeFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    if (fieldAnalyzers != null && !fieldAnalyzers.isEmpty()) {
      this.fieldAnalyzers = new HashMap<>();
      for (Map.Entry<String, Analyzer> entry : fieldAnalyzers.entrySet()) {
        // Null values are allowed in analyzers which means the default Analyzer is used
        this.fieldAnalyzers.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().getClass());
      }
    }
  }

  @Override
  public String getId() {
    return this.indexName;
  }

  @Override
  public String checkCompatibility(String regionPath, CacheServiceProfile profile) {
    String result = null;
    LuceneIndexCreationProfile myProfile = (LuceneIndexCreationProfile) profile;
    if (myProfile == null) {
      // TODO This can occur if one member defines no indexes but another one does. Currently this is caught by the async event id checks.
    } else {
      // Verify fields are the same
      if (!Arrays.equals(myProfile.getFieldNames(), getFieldNames())) {
        result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_FIELDS_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_FIELDS_3
            .toString(myProfile.getIndexName(), regionPath, Arrays.toString(getFieldNames()),
                Arrays.toString(myProfile.getFieldNames()));
      }

      // Verify the analyzer class is the same
      // Note: This test will currently only fail if per-field analyzers are used in one member but not another,
      // This condition will be caught in the tests below so this test is commented out. If we ever allow the user
      // to configure a single analyzer for all fields, then this test will be useful again.
      /*
      if (!remoteLuceneIndexProfile.getAnalyzerClass().isInstance(getAnalyzer())) {
        result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_ANALYZER_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_ANALYZER_3
            .toString(indexName, regionPath, remoteLuceneIndexProfile.getAnalyzerClass().getName(), analyzer.getClass().getName());
      }
      */

      // Verify the field analyzer fields and classes are the same if either member sets field analyzers
      if (myProfile.getFieldAnalyzers() != null || getFieldAnalyzers() != null) {
        // Check for one member defining field analyzers while the other member does not
        if (myProfile.getFieldAnalyzers() == null) {
          result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_FIELD_ANALYZERS_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_NO_FIELD_ANALYZERS
              .toString(myProfile.getIndexName(), regionPath, getFieldAnalyzers());
        } else if (getFieldAnalyzers() == null) {
          result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_NO_FIELD_ANALYZERS_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_FIELD_ANALYZERS_2
              .toString(myProfile.getIndexName(), regionPath, myProfile.getFieldAnalyzers());
        } else {
          // Both local and remote analyzers are set. Verify the sizes of the field analyzers are identical
          if (myProfile.getFieldAnalyzers().size() != getFieldAnalyzers().size()) {
            result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_FIELD_ANALYZERS_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_FIELD_ANALYZERS_3
                .toString(myProfile.getIndexName(), regionPath, getFieldAnalyzers(),
                    myProfile.getFieldAnalyzers());
          }

          // Iterate the existing analyzers and compare them to the input analyzers
          // Note: This is currently destructive to the input field analyzers map which should be ok since its a transient object.
          for (Iterator<Map.Entry<String, Class<? extends Analyzer>>> i = myProfile.getFieldAnalyzers().entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<String, Class<? extends Analyzer>> entry = i.next();
            // Remove the existing field's analyzer from the input analyzers
            Class<? extends Analyzer> analyzerClass = getFieldAnalyzers().remove(entry.getKey());

            // Verify the input field analyzer matches the current analyzer
            if (analyzerClass == null && entry.getValue() != null) {
              // The input field analyzers do not include the existing field analyzer
              result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_NO_ANALYZER_ON_FIELD_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_ANALYZER_3_ON_THAT_FIELD
                  .toString(myProfile.getIndexName(), regionPath, entry.getKey(), entry.getValue().getName());
              break;
            } else if (analyzerClass != null && entry.getValue() == null) {
              // The existing field analyzers do not include the input field analyzer
              result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_ANALYZER_2_ON_FIELD_3_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_NO_ANALYZER_ON_THAT_FIELD
                  .toString(myProfile.getIndexName(), regionPath, analyzerClass.getName(), entry.getKey());
              break;
            } else {
              if (analyzerClass != entry.getValue()) {
                // The class of the input analyzer does not match the existing analyzer for the field
                result = LocalizedStrings.LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_ANALYZER_2_ON_FIELD_3_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_ANALYZER_4_ON_THAT_FIELD
                    .toString(myProfile.getIndexName(), regionPath, analyzerClass.getName(), entry.getKey(), entry.getValue().getName());
                break;
              }
            }
          }
        }
      }
    }
    return result;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.indexName, out);
    DataSerializer.writeStringArray(this.fieldNames, out);
    DataSerializer.writeClass(this.analyzerClass, out);
    DataSerializer.writeHashMap(this.fieldAnalyzers, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.indexName = DataSerializer.readString(in);
    this.fieldNames = DataSerializer.readStringArray(in);
    this.analyzerClass = (Class<? extends Analyzer>) DataSerializer.readClass(in);
    this.fieldAnalyzers = DataSerializer.readHashMap(in);
  }

  public String toString() {
    return new StringBuilder()
        .append(getClass().getSimpleName())
        .append("[")
        .append("indexName=")
        .append(this.indexName)
        .append("; fieldNames=")
        .append(Arrays.toString(this.fieldNames))
        .append("; analyzerClass=")
        .append(this.analyzerClass)
        .append("; fieldAnalyzers=")
        .append(this.fieldAnalyzers)
        .append("]")
        .toString();
  }
}
