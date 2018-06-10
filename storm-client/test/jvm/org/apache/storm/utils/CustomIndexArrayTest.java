package org.apache.storm.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CustomIndexArrayTest {

    @Test
    public void testIndexing() {
        CustomIndexArray<String> arr;
        arr = new CustomIndexArray<>(-3, +2);
        arr.set(-3, "-three");
        arr.set(-2, "-two");
        arr.set(-1, "-one");
        arr.set(1, "+one");
        arr.set(2, "+two");

        validateContents(arr, -3, +2, "-three", "-two", "-one", null, "+one", "+two");
    }

    @Test
    public void testReverseMap() {
        CustomIndexArray<String> arr = new CustomIndexArray<>(-3, +2);
        arr.set(-3, "-three");
        arr.set(-2, "-two");
        arr.set(-1, "-one");
        arr.set(0, "zero");
        arr.set(1, "+one");
        arr.set(2, "-three");

        Map<String, List<Integer>> revMap = arr.getReverseMap();

        Assert.assertEquals(0L, (long) revMap.get("zero").get(0));
        Assert.assertEquals(1, (long) revMap.get("zero").size());

        Assert.assertEquals(-2, (long) revMap.get("-two").get(0));
        Assert.assertEquals(1, (long) revMap.get("-two").size());

        Assert.assertEquals(-3, (long) revMap.get("-three").get(0));
        Assert.assertEquals(2, (long) revMap.get("-three").get(1));
        Assert.assertEquals(2, (long) revMap.get("-three").size());

        Assert.assertEquals(1, (long) revMap.get("+one").get(0));
        Assert.assertEquals(1, (long) revMap.get("+one").size());

        Assert.assertEquals(-1, (long) revMap.get("-one").get(0));
        Assert.assertEquals(1, (long) revMap.get("-one").size());

        Assert.assertEquals(5, revMap.size());

    }

    @Test
    public void testInitializeFromMap() {
        HashMap<Integer, String> src = new HashMap<>();
        src.put(-3, "-three");
        src.put(-2, "-two");
        src.put(-1, "-one");
        src.put(1, "+one");
        src.put(2, "+two");

        CustomIndexArray<String> arr = new CustomIndexArray<>(src);

        validateContents(arr, -3, +2, "-three", "-two", "-one", null, "+one", "+two");
    }

    @Test
    public void testBothNegativeBounds() {
        //  both upper & lower bounds are -ve
        CustomIndexArray<String> arr = new CustomIndexArray<>(-5, -2);

        arr.set(-5, "-five");
        arr.set(-4, "-four");
        arr.set(-3, "-three");
        arr.set(-2, "-two");

        validateContents(arr, -5, -2, "-five", "-four", "-three", "-two");
    }

    @Test
    public void testNegativeLowerBound() {
        //  lower bound is -ve
        CustomIndexArray<String> arr = new CustomIndexArray<String>(-3, +2);
        arr.set(-3, "-three");
        arr.set(-2, "-two");
        arr.set(-1, "-one");
        arr.set(0, "zero");
        arr.set(1, "+one");
        arr.set(2, "+two");

        validateContents(arr, -3, +2, "-three", "-two", "-one", "zero", "+one", "+two");
    }

    @Test
    public void testPositiveBounds() {  // lower & upper bounds are +ve
        CustomIndexArray<String> arr = new CustomIndexArray<String>(3, 7);
        arr.set(3, "three");
        arr.set(4, "four");
        arr.set(5, "five");
        arr.set(6, "six");
        arr.set(7, "seven");

        validateContents(arr, 3, 7, "three", "four", "five", "six", "seven");
    }

    private static void validateContents(CustomIndexArray<String> arr, int lowIndex, int highIndex, String... values) {
        // 1) check size
        Assert.assertEquals(highIndex-lowIndex + 1, arr.size() );

        // 2) check valid indexes
        int vindex = 0;
        for (int i = lowIndex; i <= highIndex; i++) {
            Assert.assertEquals(values[vindex++], arr.get(i));
        }

        // 3) check out of range indexes
        try {
            arr.get(lowIndex-1);
            Assert.fail("Expected IndexOutOfBoundsException. But there wasn't one.");
        } catch (IndexOutOfBoundsException expected) {
        }

        try {
            arr.get(highIndex+1);
            Assert.fail("Expected IndexOutOfBoundsException. But there wasn't one.");
        } catch (IndexOutOfBoundsException expected) {
        }
    }


}
