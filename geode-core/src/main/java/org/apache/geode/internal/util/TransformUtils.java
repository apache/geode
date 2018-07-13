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
package org.apache.geode.internal.util;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.geode.internal.cache.persistence.PersistentMemberID;

/**
 * Contains common data tranformation utility methods and transformers.
 */
public class TransformUtils {

  /**
   * Transforms PersistentMemberIDs to a user friendly log entry.
   */
  public static final Transformer<Map.Entry<PersistentMemberID, Set<Integer>>, String> persistentMemberEntryToLogEntryTransformer =
      new Transformer<Map.Entry<PersistentMemberID, Set<Integer>>, String>() {
        @Override
        public String transform(Map.Entry<PersistentMemberID, Set<Integer>> entry) {
          PersistentMemberID memberId = entry.getKey();
          Set<Integer> bucketIds = entry.getValue();
          StringBuilder builder = new StringBuilder();
          builder.append(persistentMemberIdToLogEntryTransformer.transform(memberId));

          if (null != bucketIds) {
            builder.append("  Buckets: ");
            builder.append(bucketIds);
          }

          builder.append("\n");

          return builder.toString();
        }
      };

  /**
   * Transforms PersistentMemberIDs to a user friendly log entry.
   */
  public static final Transformer<PersistentMemberID, String> persistentMemberIdToLogEntryTransformer =
      new Transformer<PersistentMemberID, String>() {
        @Override
        public String transform(PersistentMemberID memberId) {
          StringBuilder builder = new StringBuilder();

          if (null != memberId) {
            if (null != memberId.getDiskStoreId()) {
              builder.append("\n  DiskStore ID: ");
              builder.append(memberId.getDiskStoreId().toUUID().toString());
            }

            if (null != memberId.getName()) {
              builder.append("\n  Name: ");
              builder.append(memberId.getName());
            }

            if ((null != memberId.getHost()) && (null != memberId.getDirectory())) {
              builder.append("\n  Location: ");
            }

            if (null != memberId.getHost()) {
              builder.append("/");
              builder.append(memberId.getHost().getHostAddress());
              builder.append(":");
            }

            if (null != memberId.getDirectory()) {
              builder.append(memberId.getDirectory());
            }

            builder.append("\n");
          }

          return builder.toString();
        }
      };

  /**
   * This is a simple file to file name transformer.
   */
  public static final Transformer<File, String> fileNameTransformer =
      new Transformer<File, String>() {
        public String transform(File file) {
          return file.getName();
        }
      };

  /**
   * Transforms a collection of one data type into another.
   *
   * @param from a collection of data to be transformed.
   * @param to a collection to contain the transformed data.
   * @param transformer transforms the data.
   */
  public static <T1, T2> void transform(Collection<T1> from, Collection<T2> to,
      Transformer<T1, T2> transformer) {
    for (T1 instance : from) {
      to.add(transformer.transform(instance));
    }
  }

  /**
   * Transforms a collection of one data type into another and returns a map using the transformed
   * type as the key and the original type as the value.
   *
   * @param from a collection of data to be transformed.
   * @param transformer transforms the data.
   *
   * @return a Map of transformed values that are keys to the original values.
   */
  public static <T1, T2> Map<T2, T1> transformAndMap(Collection<T1> from,
      Transformer<T1, T2> transformer) {
    Map<T2, T1> map = new HashMap<T2, T1>();
    for (T1 instance : from) {
      map.put(transformer.transform(instance), instance);
    }

    return map;
  }
}
