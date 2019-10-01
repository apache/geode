package org.apache.geode.pdx.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class TypeRegistryReverseMapTest {

  @Test
  public void saveCorrectlyAddsOnlyToReverseMaps() {
    PeerTypeRegistrationReverseMap map = new PeerTypeRegistrationReverseMap();
    assertThat(map.typeToIdSize()).isEqualTo(0);
    assertThat(map.enumToIdSize()).isEqualTo(0);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);

    addPdxTypeToMap(map);

    assertThat(map.typeToIdSize()).isEqualTo(1);
    assertThat(map.enumToIdSize()).isEqualTo(0);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);

    addEnumInfoToMap(map);

    assertThat(map.typeToIdSize()).isEqualTo(1);
    assertThat(map.enumToIdSize()).isEqualTo(1);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);

    Object fakeKey = mock(Object.class);
    Object fakeValue = mock(Object.class);
    map.save(fakeKey, fakeValue);

    assertThat(map.typeToIdSize()).isEqualTo(1);
    assertThat(map.enumToIdSize()).isEqualTo(1);
    assertThat(map.pendingTypeToIdSize()).isEqualTo(0);
    assertThat(map.pendingEnumToIdSize()).isEqualTo(0);
  }


  void addEnumInfoToMap(PeerTypeRegistrationReverseMap map) {
    EnumId enumId = mock(EnumId.class);
    EnumInfo enumInfo = mock(EnumInfo.class);
    map.save(enumId, enumInfo);
  }

  void addPdxTypeToMap(PeerTypeRegistrationReverseMap map) {
    Integer pdxId = map.typeToIdSize();
    PdxType pdxType = mock(PdxType.class);
    map.save(pdxId, pdxType);
  }
}
