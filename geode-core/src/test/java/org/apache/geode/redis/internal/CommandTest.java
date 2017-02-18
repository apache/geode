package org.apache.geode.redis.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;



/**
 * Test case for the command
 * 
 * @author Gregory Green
 *
 */
public class CommandTest {

  /**
   * Test method for {@link org.apache.geode.redis.internal.Command#Command(java.util.List)}.
   */
  @Test
  public void testCommand() {
    List<byte[]> list = null;
    try {
      new Command(list);
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
    }

    list = new ArrayList<byte[]>();
    try {
      new Command(list);
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
    }

    list = new ArrayList<byte[]>();
    list.add("Garbage".getBytes(StandardCharsets.UTF_8));

    Command cmd = new Command(list);
    Assert.assertNotNull(cmd.getCommandType());

    Assert.assertEquals(RedisCommandType.UNKNOWN, cmd.getCommandType());
    list.clear();
    list.add(RedisCommandType.HEXISTS.toString().getBytes(StandardCharsets.UTF_8));
    cmd = new Command(list);
    Assert.assertNotNull(cmd.getCommandType());
    Assert.assertNotEquals(RedisCommandType.UNKNOWN, cmd.getCommandType());
    Assert.assertEquals(RedisCommandType.HEXISTS, cmd.getCommandType());
    Assert.assertEquals(list, cmd.getProcessedCommand());
    Assert.assertNull(cmd.getKey());

    list.add("Arg1".getBytes(StandardCharsets.UTF_8));
    cmd = new Command(list);
    Assert.assertNotNull(cmd.getKey());
    Assert.assertEquals("Arg1", cmd.getStringKey());
  }



}
