package dev.learn.flink.test;

import junit.framework.TestCase;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @fileName: CleanMapFunctionTest.java
 * @description: CleanMapFunctionTest.java类说明
 * @author: huangshimin
 * @date: 2021/8/25 9:58 下午
 */
public class CleanMapFunctionTest extends TestCase {


    @Test
    public void testMap() throws Exception {
        CleanMapFunction cleanMapFunction = new CleanMapFunction();
        assertEquals("clean:3", cleanMapFunction.map("3"));
    }

    @Test
    public void testFlatMap() throws Exception {
        CleanFlatMapFunction cleanFlatMapFunction = new CleanFlatMapFunction();
        Collector<String> collector = Mockito.mock(Collector.class);
        cleanFlatMapFunction.flatMap("hsm", collector);
        Mockito.verify(collector, Mockito.times(1)).collect("flatmap:hsm");
    }
}