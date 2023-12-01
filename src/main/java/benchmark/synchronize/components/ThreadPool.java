package benchmark.synchronize.components;


import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import benchmark.synchronize.tasks.*;

import java.sql.*;

import static benchmark.oltp.OLTPClient.htapCheckQueryNumber;

class HandleThread extends Thread {
    private ThreadPool threadPool;
    private ConnPool ApConnPool;
    private ConnPool TpConnPool;
    private HTAPCheckMetrics metrics;

    // statistics
    public Long finishedTaskNum;

    private int threadId;

    public HandleThread(ThreadPool threadPool, ConnPool ApConnPool, ConnPool TpConnPool, int threadId, HTAPCheckMetrics metrics) {
        this.threadPool = threadPool;
        this.ApConnPool = ApConnPool;
        this.TpConnPool = TpConnPool;
        this.threadId = threadId;
        this.finishedTaskNum = 0L;
        this.metrics = metrics;
    }

    public void run() {
        while (threadPool.isRunning()) {
            Task task = threadPool.getTask();
            if (!threadPool.isRunning()) {
                break;
            }
            ArrayList<Connection> conns = getConns(task.taskType);
            TaskResult taskResult = task.runTask(conns, threadId);
            metrics.add(taskResult);
            threadPool.closeTask(task);
            releaseConns(task.taskType, taskResult.isApConnErr, conns);
            finishedTaskNum += 1;
        }
        System.out.printf("[%d] end\n", threadId);
    }

    private ArrayList<Connection> getConns(int taskType) {
        ArrayList<Connection> conns = new ArrayList<>();
        conns.add(ApConnPool.getConn());
        conns.add(TpConnPool.getConn());
        return conns;
    }

    private void releaseConns(int taskType, boolean isApConnErr, ArrayList<Connection> conns) {
        if (isApConnErr) {
            ApConnPool.destroyConn(conns.get(0));
        } else {
            ApConnPool.releaseConn(conns.get(0));
        }
    }
}

public class ThreadPool {
    // database connection:
    private ConnPool ApConnPool;
    private ConnPool TpConnPool;

    // task queue
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private Queue<Task> taskQueue;
    private int curTaskNum;
    private int curRunningTaskNum;

    // handle thread
    private int threadNum;
    private HandleThread handleThread[];

    // signal
    private boolean isRunning;

    // sample threads lock for batch query
    private Lock parallelThreads = new ReentrantLock();

    // batchQueryPreparing is used for prevent multi thread execute batch query task on the same (wid, did).
    private HashMap<Integer, HashMap<Integer, Boolean>> batchQueryPreparing = new HashMap<>();

    public ThreadPool(HTAPCheckInfo htapCheckInfo, Properties dbProps, HTAPCheckMetrics metrics) {
        this.isRunning = true;
        this.threadNum = htapCheckInfo.htapCheckApNum;
        this.curTaskNum = 0;
        this.curRunningTaskNum = 0;
        this.handleThread = new HandleThread[threadNum];
        this.taskQueue = new LinkedList<>();
        this.ApConnPool = new ConnPool(htapCheckInfo.htapCheckApNum + htapCheckInfo.htapCheckQueryNumber, htapCheckInfo.htapCheckApConn, dbProps);
        this.TpConnPool = new ConnPool(htapCheckInfo.htapCheckApNum + htapCheckInfo.htapCheckQueryNumber, htapCheckInfo.htapCheckTpConn, dbProps);
        for (int i = 0; i < threadNum; i++) {
            handleThread[i] = new HandleThread(this, this.ApConnPool, this.TpConnPool, i, metrics);
            handleThread[i].start();
        }
        for (int i = 1; i <= htapCheckInfo.warehouseNum; i++) {
            HashMap<Integer, Boolean> mp = new HashMap<>();
            for (int j = 1; j <= 10; ++j) {
                mp.put(j, false);
            }
            batchQueryPreparing.put(i, mp);
        }
    }

    public void trySpawn(Task task) {
        lock.lock();
        boolean canSpawn = isRunning && curTaskNum < threadNum;
        if (!canSpawn) {
            lock.unlock();
            return;
        }

        try {
            taskQueue.add(task);
            curTaskNum++;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public Task getTask() {
        lock.lock();
        try {
            while (taskQueue.isEmpty()) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (!isRunning) {
                    return null;
                }
            }
            return taskQueue.remove();
        } finally {
            lock.unlock();
        }
    }

    public void startTask() {
        lock.lock();
        curRunningTaskNum++;
        lock.unlock();
    }

    public void closeTask(Task task) {
        lock.lock();
        curRunningTaskNum--;
        curTaskNum--;
        lock.unlock();
    }

    public boolean isRunning() {
        lock.lock();
        boolean ret = isRunning;
        lock.unlock();
        return ret;
    }

    public boolean needSpawn() {
        lock.lock();
        boolean ret = isRunning && curTaskNum < threadNum;
        lock.unlock();
        return ret;
    }

    public void close() throws Throwable {
        lock.lock();
        // prevent new task.
        isRunning = false;
        lock.unlock();
        System.out.println("all task are finished");

        // there is no tasks in TaskQueue, signal all threads and let them done.
        for (int i = 0; i < threadNum; i++) {
            lock.lock();
            condition.signalAll();
            lock.unlock();
        }
        for (int i = 0; i < threadNum; i++) {
            long finishedTaskNum = handleThread[i].finishedTaskNum;
            handleThread[i].join();
            System.out.printf("[%d/%d] done, finishedTaskNum = %d\n", i, threadNum, finishedTaskNum);
        }
        ApConnPool.close();
        TpConnPool.close();
        System.out.print("close done\n");
    }
}