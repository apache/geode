package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class GMSLocatorJUnitTest {

  File tempStateFile = null;
  GMSLocator locator = null;

  @Before
  public void setUp() throws Exception {
    tempStateFile = File.createTempFile("tempLocator-", ".dat", new File("/tmp"));
    locator = new GMSLocator(null, tempStateFile, null, false, false);
    // System.out.println("temp state file: " + tempStateFile);
  }

  @After
  public void tearDown() throws Exception {
    if (tempStateFile.exists()) {
      tempStateFile.delete();
    }
  }

  private void populateStateFile(File file, int fileStamp, int ordinal, Object object) throws Exception {
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
      oos.writeInt(fileStamp);
      oos.writeInt(ordinal);
      DataSerializer.writeObject(object, oos);
    }
  }

  @Test
  public void testRecoverFromFileWithNonExistFile() throws Exception {
    tempStateFile.delete();
    assertFalse(tempStateFile.exists());
    assertFalse(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithNormalFile() throws Exception {
    NetView view = new NetView();
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL, view);
    assertTrue(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithWrongFileStamp() throws Exception {
    // add 1 to file stamp to make it invalid
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP + 1, Version.CURRENT_ORDINAL, 1);
    assertFalse(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithWrongOrdinal() throws Exception {
    // add 1 to ordinal to make it wrong
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL + 1, 1);
    assertFalse(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithInvalidViewObject() throws Exception {
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL, 1);
    try {
      locator.recoverFromFile(tempStateFile);
      fail("should catch InternalGemFileException");
    } catch (InternalGemFireException e) {
      assertTrue(e.getMessage().startsWith("Unable to recover previous membership view from"));
    }
  }

}

