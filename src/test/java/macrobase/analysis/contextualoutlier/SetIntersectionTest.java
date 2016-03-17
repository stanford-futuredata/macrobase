package macrobase.analysis.contextualoutlier;

import java.util.*;
import java.util.stream.Collectors;

public class SetIntersectionTest {

    static Random rng = new Random();

    static abstract class RunIt {
        public long count;
        public long nsTime;
        abstract int Run (Set<Long> s1, Set<Long> s2);
    }

    // As presented in the post
    static class PostMethod extends RunIt {
        public int Run(Set<Long> set1, Set<Long> set2) {
            boolean set1IsLarger = set1.size() > set2.size();
            Set<Long> cloneSet = new HashSet<Long>(set1IsLarger ? set2 : set1);
            cloneSet.retainAll(set1IsLarger ? set1 : set2);
            return cloneSet.size();
        }
    }

    // No intermediate HashSet
    static class MyMethod1 extends RunIt {
        public int Run (Set<Long> set1, Set<Long> set2) {
            Set<Long> a;
            Set<Long> b;
            if (set1.size() <= set2.size()) {
                a = set1;
                b = set2;           
            } else {
                a = set2;
                b = set1;
            }
            int count = 0;
            for (Long e : a) {
                if (b.contains(e)) {
                    count++;
                }           
            }
            return count;
        }
    }

    // With intermediate HashSet
    static class MyMethod2 extends RunIt {
        public int Run (Set<Long> set1, Set<Long> set2) {
            Set<Long> a;
            Set<Long> b;
            Set<Long> res = new HashSet<Long>();
            if (set1.size() <= set2.size()) {
                a = set1;
                b = set2;           
            } else {
                a = set2;
                b = set1;
            }
            for (Long e : a) {
                if (b.contains(e)) {
                    res.add(e);
                }           
            }
            return res.size();
        }
    }
    
 // With intermediate List
    static class MyMethod3 extends RunIt {
        public int Run (Set<Long> set1, Set<Long> set2) {
            Set<Long> a;
            Set<Long> b;
            List<Long> res = new ArrayList<Long>();
            if (set1.size() <= set2.size()) {
                a = set1;
                b = set2;           
            } else {
                a = set2;
                b = set1;
            }
            for (Long e : a) {
                if (b.contains(e)) {
                    res.add(e);
                }           
            }
            return res.size();
        }
    }
    
    
 // With intermediate List
    static class MyMethod4 extends RunIt {
        public int Run (Set<Long> set1, Set<Long> set2) {
        	List<Long> result = set1.stream().filter(s -> set2.contains(s)).collect(Collectors.toList());
        	return result.size();
        }
    }

    static Set<Long> makeSet (int count, float load) {
        Set<Long> s = new HashSet<Long>();
        for (int i = 0; i < count; i++) {
            s.add((long)rng.nextInt(Math.max(1, (int)(count * load))));                     
        }
        return s;
    }

    // really crummy ubench stuff
    public static void main(String[] args) {
        int[][] bounds = {
                {1, 1},
                {1, 10},
                {1, 100},
                {1, 1000},
                {10, 2},
                {10, 10},
                {10, 100},
                {10, 1000},
                {100, 1},
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
                                new PostMethod(),
                                new MyMethod1(),
                                new MyMethod2(),
                                new MyMethod3(),
                                new MyMethod4()));
                for (int r = 0; r < cycleReps; r++) {
                    ArrayList<RunIt> runs = new ArrayList<RunIt>(allRuns);
                    Set<Long> set1 = makeSet(set1size, load);
                    Set<Long> set2 = makeSet(set2size, load);
                    while (runs.size() > 0) {
                        int runIdx = rng.nextInt(runs.size());
                        RunIt run = runs.remove(runIdx);
                        long start = System.nanoTime();
                        int count = 0;
                        for (int s = 0; s < subReps; s++) {
                            count += run.Run(set1, set2); 
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