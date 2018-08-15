package org.apache.geode.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.management.cli.CliFunctionResult;

public interface FunctionResult extends Comparable<CliFunctionResult>, DataSerializableFixedID {
  /**
   * Remove elements from the list that are not instances of CliFunctionResult and then sort the
   * results.
   *
   * @param results The results to clean.
   * @return The cleaned results.
   */
  static List<CliFunctionResult> cleanResults(List<?> results) {
    List<CliFunctionResult> returnResults = new ArrayList<CliFunctionResult>(results.size());
    for (Object result : results) {
      if (result instanceof CliFunctionResult) {
        returnResults.add((CliFunctionResult) result);
      }
    }

    Collections.sort(returnResults);
    return returnResults;
  }

  String getMemberIdOrName();

  @Deprecated
  String getMessage();

  String getStatus();

  String getStatusMessage();

  @Deprecated
  Serializable[] getSerializables();

  @Deprecated
  Throwable getThrowable();

  Object getResultObject();

  @Override
  int getDSFID();

  @Override
  void toData(DataOutput out) throws IOException;

  @Override
  void fromData(DataInput in) throws IOException, ClassNotFoundException;

  boolean isSuccessful();

  @Deprecated
  byte[] getByteData();

  @Override
  int compareTo(CliFunctionResult o);

  @Override
  int hashCode();

  @Override
  boolean equals(Object obj);

  @Override
  String toString();

  @Override
  Version[] getSerializationVersions();
}
