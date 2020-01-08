package org.apache.geode.internal.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.VersionedDataInputStream;

public class EventIDTest {

  @Test
  public void emptyEventIdCanBeSerializedWithCurrentVersion()
      throws IOException, ClassNotFoundException {
    emptyEventIdCanBeSerialized(Version.CURRENT);

  }

  @Test
  public void emptyEventIdCanBeSerializedToGeode100() throws IOException, ClassNotFoundException {
    emptyEventIdCanBeSerialized(Version.GFE_90);
  }

  private void emptyEventIdCanBeSerialized(Version version)
      throws IOException, ClassNotFoundException {
    EventID eventID = new EventID();
    HeapDataOutputStream out = new HeapDataOutputStream(version);
    DataSerializer.writeObject(eventID, out);

    EventID result = DataSerializer.readObject(
        new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()), version));

    Assertions.assertThat(result.getMembershipID()).isEqualTo(eventID.getMembershipID());
  }

}
