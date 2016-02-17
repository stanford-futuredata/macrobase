package macrobase.analysis.sample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * See http://arxiv.org/pdf/1012.0256.pdf
 */
class AChao<T> {
    private final List<T> reservoir;
    double runningCount;
    private final int reservoirCapacity;
    private Random random = new Random();

    public void setSeed(long seed) {
        random = new Random(seed);
    }

    public AChao(int capacity) {
        reservoir = new ArrayList<>();
        reservoirCapacity = capacity;
    }

    public final List<T> getReservoir() {
        return reservoir;
    }

    public void insertBatch(Collection<T> elements, double weight) {
        for (T ele : elements) {
            insert(ele, weight);
        }
    }

    protected void setRunningCount(double newCount) {
        runningCount = newCount;
    }

    protected double getRunningCount() {
        return runningCount;
    }

    public void insert(T ele, double weight) {
        runningCount += weight;

        if (reservoir.size() < reservoirCapacity) {
            reservoir.add(ele);
        } else if (random.nextDouble() < weight / runningCount) {
            reservoir.remove(random.nextInt(reservoirCapacity));
            reservoir.add(ele);
        }
    }
}
