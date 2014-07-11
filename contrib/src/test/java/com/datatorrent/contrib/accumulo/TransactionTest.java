package com.datatorrent.contrib.accumulo;

import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class TransactionTest {
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new AccumuloApp(), 30000);
  }
}
