package macrobase.util;


import org.apache.commons.math3.linear.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.contextualoutlier.Context;

import static junit.framework.TestCase.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.BitSet;

public class BitSetUtilTest {

   
    @Test
    public void test1() throws IOException {
        BitSet bs1 = new BitSet(10);
        bs1.set(0);
        bs1.set(2);
        bs1.set(5);
        FileOutputStream fos = new FileOutputStream("bs1.txt");
        fos.write(bs1.toByteArray());
        fos.close();
        System.out.println(bs1.size());
        //System.out.println(sb);
    }

    
    @Test
    public void test2() {
        BitSet outlierBitSet = new BitSet(10);
        outlierBitSet.set(0);
        outlierBitSet.set(2);
        outlierBitSet.set(5);
        
        BitSet bs2 = new BitSet(10);
        bs2.set(0);
        bs2.set(2);
        bs2.set(4);
        bs2.set(5);
        bs2.set(6);
        
        assertEquals(BitSetUtil.subSetByBitSet(outlierBitSet, bs2, 1.0),true);
        
        double coverage = BitSetUtil.subSetCoverage(outlierBitSet, bs2);
        System.out.println("coverage: " + coverage);
        //assertEquals(coverage, 1.0);
        
        int containedBit = 0;
        int missedBit = 0;
        for (int i = outlierBitSet.nextSetBit(0); i >= 0; i = outlierBitSet.nextSetBit(i + 1)) {
            if (bs2.get(i)) {
                containedBit++;
            } else {
                missedBit++;
            }
            
        }
        double missedPercentage = (double)(missedBit) / (missedBit + containedBit);
        System.out.println("missed: " + missedPercentage);

        assertEquals(missedPercentage, 0.0);


    }
}
