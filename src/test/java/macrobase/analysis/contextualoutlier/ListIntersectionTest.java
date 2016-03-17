package macrobase.analysis.contextualoutlier;

import java.util.*;
import java.util.stream.Collectors;

public class ListIntersectionTest {

    static int maxSize = 1000;

    
    static Random rng = new Random();

    static abstract class RunIt {
        public long count;
        public long nsTime;
        abstract List<Integer> Run (List<Integer> s1, List<Integer> s2);
    }

    // No intermediate HashSet
    static class SetIntersection extends RunIt {

		@Override
		List<Integer> Run(List<Integer> s1, List<Integer> s2) {
			List<Integer> result = new ArrayList<Integer>();
			HashSet<Integer> temp1 = new HashSet<Integer>(s1);
			HashSet<Integer> temp2 = new HashSet<Integer>(s2);
			for(Integer t: temp1){
				if(temp2.contains(t)){
					result.add(t);
				}
			}
			return result;
		}
       
    }

  
    
 // With intermediate List
    static class BitSetOperation extends RunIt {

		@Override
		List<Integer> Run(List<Integer> s1, List<Integer> s2) {
			
			BitSet b1 = new BitSet(maxSize);
			for(int i = 0; i < s1.size(); i++){
				int l =  s1.get(i);
				b1.set(l);
			}
			
			BitSet b2 = new BitSet(maxSize);
			for(int i = 0; i < s2.size(); i++){
				int l = s2.get(i);
				b2.set(l);;
			}
			
			b1.and(b2);
			
			List<Integer> result = new ArrayList<Integer>();
			//To iterate over the true bits in a BitSet, use the following loop:
			for (int i = b1.nextSetBit(0); i >= 0; i = b1.nextSetBit(i+1)) {
			     // operate on index i here
				 result.add(i);
			     if (i == Integer.MAX_VALUE) {
			         break; // or (i+1) would overflow
			     }
			 }
			
			return result;
			
		}
      
    }
    
   

    static List<Integer> makeList (int count, int max) {
        List<Integer> result = new ArrayList<Integer>();
        Random rnd = new Random();
        for (int i = 0; i < count; i++) {
            //probabilty of keeping it
        	if( rnd.nextInt(max) <= count ){
        		result.add(new Integer(i));
        	}
        }
        return result;
    }

    // really crummy ubench stuff
    public static void main(String[] args) {
    	
    	BitSetOperation bs = new BitSetOperation();
    	List<Integer> bsresult = bs.Run(Arrays.asList(1,2,3,100,1000), Arrays.asList(1000,2,3,4));
    	
        int[][] bounds = {
                {10, 2},
                {10, 10},
                {10, 100},
                {10, 1000},
                {100, 10},
                {100, 100},
                {100, 1000},
        };
        
        int totalReps = 4;
        int cycleReps = 1000;
        int subReps = 1000;
        float load = 0.8f;
        for (int tc = 0; tc < totalReps; tc++) {
            for (int[] bound : bounds) {
                int set1size = bound[0];
                int set2size = bound[1];
                System.out.println("Running tests for " + set1size + "x" + set2size);               
                ArrayList<RunIt> allRuns = new ArrayList<RunIt>(
                        Arrays.asList(
                                new SetIntersection(),
                                new BitSetOperation()));
                for (int r = 0; r < cycleReps; r++) {
                    ArrayList<RunIt> runs = new ArrayList<RunIt>(allRuns);
                    List<Integer> set1 = makeList(set1size, maxSize);
                    List<Integer> set2 = makeList(set2size, maxSize);
                    while (runs.size() > 0) {
                        int runIdx = rng.nextInt(runs.size());
                        RunIt run = runs.remove(runIdx);
                        long start = System.nanoTime();
                        int count = 0;
                        for (int s = 0; s < subReps; s++) {
                            count += run.Run(set1, set2).size(); 
                        }                       
                        long time = System.nanoTime() - start;
                        run.nsTime += time;
                        run.count += count;
                    }
                }
                for (RunIt run : allRuns) {
                    double sec = run.nsTime / (10e6);
                    System.out.println(run + " took " + sec + " count=" + run.count);
                }
            }
        }       
    }
}