package benchmark.synchronize.components;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import benchmark.synchronize.tasks.TaskResult;

public class HTAPCheckMetrics {
    private BufferedWriter runInfoCSV = null;
    private final Lock lock = new ReentrantLock();

    public HTAPCheckMetrics(String resultDirectory) {
        File resultDir = new File(resultDirectory);
        String runInfoCSVName = new File(resultDir, "runInfo.csv").getPath();
        try {
            if (!resultDir.mkdir()) {
				System.out.printf("mkdir fails\n");
				System.exit(1);
			}
            runInfoCSV = new BufferedWriter(new FileWriter(runInfoCSVName));
            runInfoCSV.write("taskType,txnCompleteTime,gapTime,startTime,endTime,tryNum,pass,isApConnErr\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void add(TaskResult taskResult) {
        StringBuffer infoSB = new StringBuffer();
		Formatter infoFmt = new Formatter(infoSB);
		infoFmt.format("%d,%d,%d,%d,%d,%d,%s,%s\n",
						taskResult.taskType,
                        taskResult.txnCompleteTime,
                        taskResult.gapTime,
                        taskResult.startTime,
                        taskResult.endTime,
                        taskResult.tryNum,
                        taskResult.pass,
                        taskResult.isApConnErr);
        try {
            lock.lock();
            runInfoCSV.write(infoSB.toString());
            runInfoCSV.flush();
            lock.unlock();
        } catch (Exception e) {
            lock.unlock();
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            runInfoCSV.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
