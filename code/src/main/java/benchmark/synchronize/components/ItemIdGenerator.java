package benchmark.synchronize.components;

import java.util.*;

public class ItemIdGenerator {
    private Random random;
    private static long nURandCI_ID;
    private HashSet<Integer> visit;

    public ItemIdGenerator() {
        this.random = new Random(System.nanoTime());
        nURandCI_ID = nextLong(0, 8191);
        visit = new HashSet<>();
    }

    public long nextLong(long x, long y) {
        return (long) (random.nextDouble() * (y - x + 1) + x);
    }

    public int generateItemId() {
        return (int) ((((nextLong(0, 8191) | nextLong(1, 100000)) + nURandCI_ID) % 100000) + 1);
    }

    public int getItemId() {
        int itemId;
        do {
            itemId = generateItemId();
        } while (visit.contains(itemId));
        visit.add(itemId);
        return itemId;
    }

    public void releaseItemId(int itemId) {
        visit.remove(itemId);
    }
}
