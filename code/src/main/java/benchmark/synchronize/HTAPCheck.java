package benchmark.synchronize;

import java.util.*;

import benchmark.synchronize.components.*;
import benchmark.synchronize.tasks.*;

public class HTAPCheck {
    // htap check variables
    public HTAPCheckInfo info;

    // thread pool
    private ThreadPool threadPool;

    // rnd is used for htapCheckCrossFrequency.
    private Random rnd;

    // nextOrderIdGenerator is used for generate order id atomically to replace `select .. for update`.
    private NextOrderIdGenerator nextOrderIdGenarator;

    // nextOrderIdGenerator is used for generate item id to replace `select .. for update`.
    private ItemIdGenerator itemIdGenerator;

    private HTAPCheckMetrics metrics;

    public HTAPCheck(HTAPCheckInfo htapCheckInfo, Properties dbProps) {
        this.info = htapCheckInfo;
        this.metrics = new HTAPCheckMetrics(htapCheckInfo.resultDir);
        threadPool = new ThreadPool(htapCheckInfo, dbProps, metrics);
        rnd = new Random();
    }

    public void trySpawn(Task task) {
        threadPool.trySpawn(task);
    }

    public boolean needSpawn() {
        return threadPool.needSpawn();
    }

    public int getRandomDid() {
        return rnd.nextInt(10) + 1;
    }

    public int getNextOrderId(int warehouseId, int districtId) {
        return nextOrderIdGenarator.getOrderId(warehouseId, districtId);
    }

    public synchronized int getItemId() {
        return itemIdGenerator.getItemId();
    }

    public synchronized void releaseItemId(int itemId) {
        itemIdGenerator.releaseItemId(itemId);
    }

    protected void close() throws Throwable {
        threadPool.close();
        metrics.close();
    }
}
