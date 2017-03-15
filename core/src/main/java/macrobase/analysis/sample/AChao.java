package macrobase.analysis.sample;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * See http://arxiv.org/pdf/1012.0256.pdf
 */
class AChao<T> {
    private class OverweightItem<T> implements Comparable<OverweightItem> {
        public Double weight;
        public T item;

        public OverweightItem(T item,
                              double weight) {
            this.item = item;
            this.weight = weight;
        }

        @Override
        public int compareTo(OverweightItem o) {
            return weight.compareTo(o.weight);
        }
    }

    private final List<T> reservoir;
    double runningCount;
    private final int reservoirCapacity;
    private final Random random;
    private final PriorityQueue<OverweightItem<T>> overweightItems = new PriorityQueue<>();

    public AChao(int capacity) {
        this(capacity, new Random());
    }

    public AChao(int capacity, Random random) {
        reservoir = new ArrayList<>();
        reservoirCapacity = capacity;
        this.random = random;
    }

    private void updateOverweightItems() {
        while(!overweightItems.isEmpty()) {
            OverweightItem<T> ow = overweightItems.peek();
            if(reservoirCapacity * ow.weight / runningCount <= 1) {
                overweightItems.poll();
                insert(ow.item, ow.weight);
            } else {
                break;
            }
        }
    }

    public final List<T> getReservoir() {
        updateOverweightItems();

        if(!overweightItems.isEmpty()) {
            // overweight items always make it in the sample
            List<T> ret = overweightItems.stream().map(i -> i.item).collect(Collectors.toList());

            assert (ret.size() <= reservoirCapacity);

            // fill the return value with a sample of non-overweight elements
            Collections.shuffle(reservoir, random);
            ret.addAll(reservoir.subList(0, reservoirCapacity-ret.size()));
            return ret;
        }

        return reservoir;
    }

    protected void decayWeights(double decay) {
        runningCount *= decay;
        overweightItems.forEach(i -> i.weight *= decay);
    }

    public void insert(T ele, double weight) {
        runningCount += weight;

        updateOverweightItems();

        if (reservoir.size() < reservoirCapacity) {
            reservoir.add(ele);
        } else {
            double pInsertion = reservoirCapacity * weight / runningCount;

            if(pInsertion > 1) {
                overweightItems.add(new OverweightItem(ele, weight));
            } else if (random.nextDouble() < pInsertion) {
                reservoir.set(random.nextInt(reservoirCapacity), ele);
            }
        }
    }
}
