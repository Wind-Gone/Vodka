package bean;

import java.util.*;
import java.util.concurrent.*;

public class ReservoirSampling {
    private final ConcurrentHashMap<Integer, OrderLine> reservoir;
    private final int sampleSize;
    private final ConcurrentSkipListSet<OrderLine> orderedSet = new ConcurrentSkipListSet<>(Comparator.comparing(OrderLine::getOl_receipdate));

    public ReservoirSampling(int sampleSize) {
        this.reservoir = new ConcurrentHashMap<>();
        this.sampleSize = sampleSize;
    }

    public void addOrderLine(OrderLine orderLine) {
        int currentIndex = reservoir.size();
        if (currentIndex < sampleSize) {
            reservoir.put(currentIndex, orderLine);
            orderedSet.add(orderLine);
        } else {
            int randomIndex = ThreadLocalRandom.current().nextInt(currentIndex + 1);
            if (randomIndex < sampleSize) {
                OrderLine removed = reservoir.remove(randomIndex);
                orderedSet.remove(removed);
                reservoir.put(randomIndex, orderLine);
                orderedSet.add(orderLine);
            }
        }
    }

    public OrderLine getOrderLine(double selectivity) {
        int index = (int) (selectivity * sampleSize);
        if (index >= 0 && index < sampleSize) {
            Iterator<OrderLine> iterator = orderedSet.iterator();
            int currentIndex = 0;
            while (iterator.hasNext()) {
                OrderLine orderLine = iterator.next();
                if (currentIndex == index) {
                    return orderLine;
                }
                currentIndex++;
            }
        }
        return null;
    }
}
