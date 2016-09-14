package macrobase.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BitSetUtil {
    public static BitSet indexes2BitSet(List<Integer> indexes, int total) {
        BitSet bs = new BitSet(total);
        for (int i = 0; i < indexes.size(); i++) {
            int index = indexes.get(i);
            bs.set(index);
        }
        return bs;
    }

    public static List<Integer> bitSet2Indexes(BitSet bs) {
        List<Integer> indexes = new ArrayList<Integer>();
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            if (i == Integer.MAX_VALUE) {
                break; // or (i+1) would overflow
            }
            // operate on index i here
            indexes.add(i);
            
        }
        return indexes;
    } 
    /**
     * bs1 a subset of bs2 iff more than threshold of bs1 is in bs2
     * @param bs1
     * @param bs2
     * @return
     */
    public static boolean subSetByBitSet(BitSet bs1, BitSet bs2, double threshold) {
        
        double covered = subSetCoverage(bs1,bs2);
        if (covered >= threshold) {
            return true;
        } else {
            return false;
        }
    }
    /**
     * Percentage of bits in bs1, that are set in bs2
     * @param bs1
     * @param bs2
     * @return
     */
    public static double subSetCoverage(BitSet bs1, BitSet bs2) {
        if (bs1.cardinality() == 0) {
           return 1.0;
        }
        BitSet bsClone = (BitSet)bs1.clone();
        bsClone.andNot(bs2);
        
        int notContained = bsClone.cardinality();
        double covered = (double)(bs1.cardinality() - notContained) / bs1.cardinality();
        return covered;
    }
    
}
