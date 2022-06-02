package org.apache.geode.internal.cache.event;

import org.apache.geode.internal.cache.AbstractRegionMapTest;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BulkOperationHolderTest {

  @Test
  void putVersionTag() {
  }

  @Test
  void getEntryVersionTags() {
  }

  @Test
  void testToStringEmpty() {
    BulkOperationHolder obj = new BulkOperationHolder();
    assertEquals("BulkOperationHolder tags={}", obj.toString());
  }

  @Test
  void testToStringWithContent() {
    BulkOperationHolder obj = new BulkOperationHolder();
    EventID event = mock(EventID.class);
    VersionTag tag = mock(VMVersionTag.class);
    when(event.toString()).thenReturn("mocked event");
    when(tag.toString()).thenReturn("mocked version tag");
    obj.putVersionTag(event, tag);
    assertEquals("BulkOperationHolder tags={mocked event=mocked version tag}", obj.toString());
    EventID event2 = mock(EventID.class);
    VersionTag tag2 = mock(VMVersionTag.class);
    when(event2.toString()).thenReturn("mocked event2");
    when(tag2.toString()).thenReturn("mocked version tag2");
    obj.putVersionTag(event2, tag2);
    String expected = "BulkOperationHolder tags={mocked event=mocked version tag, mocked event2=mocked version tag2}";
//    String expected = "BulkOperationHolder tags={mocked event2=mocked version tag2, mocked event2=mocked version tag2}";
    assertEquals(expected, obj.toString());
  }

  @Test
  void expireNowIsBeforeExpiration() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(1, 2);
    assertEquals(true, obj.isRemoved());
  }

  @Test
  void expireNowIsEqualToExpriation() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(2, 2);
    assertEquals(true, obj.isRemoved());
  }

  @Test
  void expireNowIsAfterExpriation() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(3, 2);
    assertEquals(false, obj.isRemoved());
  }

  @Test
  void expireWalkNowTimeBackward() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(5, 2);
    assertEquals(false, obj.isRemoved());
    obj.expire(4, 2);
    assertEquals(false, obj.isRemoved());
    obj.expire(3, 2);
    assertEquals(false, obj.isRemoved());
    obj.expire(2, 2);
    assertEquals(false, obj.isRemoved());
    obj.expire(1, 2);
    assertEquals(false, obj.isRemoved());
  }

  @Test
  void expireWalkNowTimeForward() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(1, 2);
    assertEquals(true, obj.isRemoved());
    obj.expire(2, 2);
    assertEquals(true, obj.isRemoved());
    obj.expire(3, 2);
    assertEquals(true, obj.isRemoved());
    obj.expire(4, 2);
    assertEquals(true, obj.isRemoved());
    obj.expire(5, 2);
    assertEquals(true, obj.isRemoved());
  }

  @Test
  void expireWalkNowTimeForwardStartAfterExpiration() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(10, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(12, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(13, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(14, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(15, 8);
    assertEquals(false, obj.isRemoved());
  }

  @Test
  void expireWalkNowTimeBackwardStartAfterExpiration() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(10, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(9, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(8, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(7, 8);
    assertEquals(false, obj.isRemoved());
    obj.expire(6, 8);
    assertEquals(false, obj.isRemoved());
  }

  @Test
  void expireWalkExpiryBackwardStartAfterNow() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(8, 10);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 9);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 8);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 7);
    assertEquals( true, obj.isRemoved());
    obj.expire(8, 6);
    assertEquals(true, obj.isRemoved());
  }

  @Test
  void expireWalkExpiryForwardStartAfterNow() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(8, 10);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 11);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 12);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 13);
    assertEquals( true, obj.isRemoved());
    obj.expire(8, 14);
    assertEquals(true, obj.isRemoved());
  }

  @Test
  void expireWalkExpiryForwardStartBeforeNow() {
    BulkOperationHolder obj = new BulkOperationHolder();
    obj.expire(8, 6);
    assertEquals(false, obj.isRemoved());
    obj.expire(8, 7);
    assertEquals(false, obj.isRemoved());
    obj.expire(8, 8);
    assertEquals(true, obj.isRemoved());
    obj.expire(8, 9);
    assertEquals( true, obj.isRemoved());
    obj.expire(8, 10);
    assertEquals(true, obj.isRemoved());
  }

  @Test
  void isRemoved() {
  }
}
