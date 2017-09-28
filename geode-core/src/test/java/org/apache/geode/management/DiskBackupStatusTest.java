package org.apache.geode.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;

public class DiskBackupStatusTest {

  @Test
  public void valuesNotSetTest() {
    DiskBackupStatus status = new DiskBackupStatus();
    assertThat(status.getBackedUpDiskStores()).isNull();
    assertThat(status.getOfflineDiskStores()).isNull();
  }

  @Test
  public void returnsSetValues() {
    DiskBackupStatus status = new DiskBackupStatus();
    String[] testOfflineDiskStores = new String[] {"test", "array"};
    status.setOfflineDiskStores(testOfflineDiskStores);
    assertThat(status.getOfflineDiskStores()).isEqualTo(testOfflineDiskStores);

    Map<String, String[]> testBackedUpDiskStores = new HashMap<>();
    testBackedUpDiskStores.put("key1", new String[] {"value1"});
    status.setBackedUpDiskStores(testBackedUpDiskStores);
    assertThat(status.getBackedUpDiskStores()).isEqualTo(testBackedUpDiskStores);
  }

  @Test
  public void generatesCorrectBackupUpDiskStores() {
    Map<DistributedMember, Set<PersistentID>> backedUpDiskStores = new HashMap<>();

    DistributedMember member1 = generateTestMember("member1");
    Set<PersistentID> idSet1 = generateTestIDs(1);
    backedUpDiskStores.put(member1, idSet1);

    DistributedMember member2 = generateTestMember("member2");
    Set<PersistentID> idSet2 = generateTestIDs(2);
    backedUpDiskStores.put(member2, idSet2);

    DiskBackupStatus status = new DiskBackupStatus();
    status.generateBackedUpDiskStores(backedUpDiskStores);

    Map<String, String[]> storedDiskStores = status.getBackedUpDiskStores();
    assertThat(storedDiskStores).containsOnlyKeys("member1", "member2");
    assertThat(storedDiskStores.get("member1").length).isEqualTo(1);
    assertThat(storedDiskStores.get("member2").length).isEqualTo(2);
    assertThat(storedDiskStores.get("member2")).contains("DirectoryForId0", "DirectoryForId1");
  }

  @Test
  public void generatesCorrectOfflineDiskStores() {
    Set<PersistentID> ids = generateTestIDs(2);
    DiskBackupStatus status = new DiskBackupStatus();
    status.generateOfflineDiskStores(ids);

    String[] storedIds = status.getOfflineDiskStores();
    assertThat(storedIds.length).isEqualTo(2);
    assertThat(storedIds).contains("DirectoryForId0", "DirectoryForId1");
  }

  private DistributedMember generateTestMember(String name) {
    DistributedMember member = mock(DistributedMember.class);
    when(member.getId()).thenReturn(name);
    return member;
  }

  private Set<PersistentID> generateTestIDs(int idsToGenerate) {
    Set<PersistentID> ids = new HashSet<>();
    for (int i = 0; i < idsToGenerate; i++) {
      PersistentID id = mock(PersistentID.class);
      when(id.getDirectory()).thenReturn("DirectoryForId" + i);
      ids.add(id);
    }
    return ids;
  }
}