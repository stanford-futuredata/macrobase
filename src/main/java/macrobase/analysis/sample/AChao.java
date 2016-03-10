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
    private final Random random;

    public AChao(int capacity) {
        this(capacity, new Random());
    }

    public AChao(int capacity, Random random) {
        reservoir = new ArrayList<>();
        reservoirCapacity = capacity;
        this.random = random;
    }

    public final List<T> getReservoir() {
        return reservoir;
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
