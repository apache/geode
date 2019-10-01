package org.apache.geode.pdx.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TypeRegistryReverseMap {
  /**
   * These maps allow revere look-ups of the idToType and idToEnum maps (or the idToType region
   * if the TypeRegistration containing them is a PeerTypeRegistration) without having to iterate
   * through the entry set
   */
  final Map<PdxType, Integer> typeToId = Collections.synchronizedMap(new HashMap<>());
  final Map<EnumInfo, EnumId> enumToId = Collections.synchronizedMap(new HashMap<>());

  void save(Object key, Object value) {
    if (value instanceof PdxType) {
      PdxType type = (PdxType) value;
      typeToId.put(type, (Integer) key);
    } else if (value instanceof EnumInfo) {
      EnumInfo info = (EnumInfo) value;
      enumToId.put(info, (EnumId) key);
    }
  }

  int typeToIdSize() {
    return typeToId.size();
  }

  int enumToIdSize() {
    return enumToId.size();
  }

  Integer getIdFromReverseMap(PdxType newType) {
    return typeToId.get(newType);
  }

  EnumId getIdFromReverseMap(EnumInfo newInfo) {
    return enumToId.get(newInfo);
  }
}
