package org.apache.geode.connectors.util.internal;

import java.util.Map;

public class DescribeMappingResult {
  private Map<String, String> attributeMap;

  public DescribeMappingResult(Map<String, String> attributeMap) {
    this.attributeMap = attributeMap;
  }

  public Map<String, String> getAttributeMap() {
    return this.attributeMap;
  }
}
