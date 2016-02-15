package macrobase;

import java.util.concurrent.TimeUnit;

import macrobase.runtime.MacroBaseServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.lang.Math;

import com.google.common.base.Stopwatch;

/**
 * Hello world!
 *
 */
public class MacroBase
{
    public static final MetricRegistry metrics = new MetricRegistry();
    public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                                                    .convertRatesTo(TimeUnit.SECONDS)
                                                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                    .build();

    private static Semaphore startSemaphore;
    private static Semaphore endSemaphore;

    @SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(MacroBase.class);

    static class RunnableStreamingAnalysis implements Runnable {
        int numThreads;
        int sharedDataArrayLength;
        int[] sharedDataArray;

        RunnableStreamingAnalysis(int sharedDataArrayLength,
                                  int numThreads) {
            this.numThreads = numThreads;
            this.sharedDataArray = new int[sharedDataArrayLength];
            this.sharedDataArrayLength = sharedDataArrayLength;
        }

        @Override
        public void run() {
            int a = 1;
            Stopwatch sw = Stopwatch.createUnstarted();
            Stopwatch tsw = Stopwatch.createUnstarted();
            tsw.start();
            try {
                startSemaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sw.start();
            long numIterations = ((128 * 3) / numThreads);
            for (long i = 0; i < numIterations; i++) {
              for (int j = 0; j < sharedDataArrayLength; j++) {
                sharedDataArray[j] *= i;
                sharedDataArray[j] -= i;
                sharedDataArray[j] = (int) Math.pow(sharedDataArray[j], 2);
                for (int k = 0; k < 10; k++)
                  sharedDataArray[j] = (int) Math.pow(sharedDataArray[j], 1.1);
              }
            }
            sw.stop();
            log.debug("Only-computation time: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
            endSemaphore.release();
            tsw.stop();
            log.debug("Total time: {}ms", tsw.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    public static void main( String[] args ) throws Exception
    {
        System.out.println("Welcome to\n" +
                           "  _   _   _   _   _   _   _   _   _  \n" +
                           " / \\ / \\ / \\ / \\ / \\ / \\ / \\ / \\ / \\ \n" +
                           "( m | a | c | r | o | b | a | s | e )\n" +
                           " \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \n");

        //benchmark();

        int allNumThreads[] = {1, 2, 4, 8, 16, 24, 32};
        for (int numThreads : allNumThreads) {
          log.debug("Running experiment for numThreads = {}", numThreads);

          Stopwatch tsw = Stopwatch.createUnstarted();
          tsw.start();

          startSemaphore = new Semaphore(0);
          endSemaphore = new Semaphore(0);

          for (int i = 0; i < numThreads; i++) {
                  int sharedDataArrayLength = 10000000;
                  RunnableStreamingAnalysis rsa = new RunnableStreamingAnalysis(
                                  sharedDataArrayLength,
                                  numThreads);
                  Thread t = new Thread(rsa);
                  t.start();
          }

          startSemaphore.release(numThreads);
          endSemaphore.acquire(numThreads);

          tsw.stop();
        }

        // MacroBaseServer.main(args);
    }
}
