/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.engine.TestCountSink;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.testbench.HashMapToKeyValPair}<p>
 * <br>
 */
public class HashMapToKeyValPairBenchmark
{
  private static Logger log = LoggerFactory.getLogger(HashMapToKeyValPairBenchmark.class);

  /**
   * Test oper pass through. The Object passed is not relevant
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    HashMapToKeyValPair oper = new HashMapToKeyValPair();
    TestCountSink keySink = new TestCountSink();
    TestCountSink valSink = new TestCountSink();
    TestCountSink keyvalSink = new TestCountSink();

    oper.key.setSink(keySink);
    oper.val.setSink(valSink);
    oper.keyval.setSink(keyvalSink);
    oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

    oper.beginWindow(0);
    HashMap<String, String> input = new HashMap<String, String>();
    input.put("a", "1");
    // Same input object can be used as the oper is just pass through
    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input);
    }

    oper.endWindow();
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples));
  }
}