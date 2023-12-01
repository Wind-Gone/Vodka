package utils.common;

import org.apache.log4j.Logger;

import java.sql.SQLException;

public class TPMonitor implements Runnable {
    private static org.apache.log4j.Logger log = Logger.getLogger(TPMonitor.class);
    double threshold;
    static int apNumber;

    public TPMonitor(int limPerMin, double TPthreshold, int apNumber) {
        this.threshold = limPerMin * (1 - TPthreshold);
        TPMonitor.apNumber = apNumber;
    }

    @Override
    public void run() {
        boolean systemNotStopRunning = true;

        try {
            Thread.sleep(120000);        // warmup2分钟
            while (systemNotStopRunning) {
                if (benchmark.oltp.OLTPClient.getSignalTerminalsRequestEndSent())
                    break;
                double realTimeTPS = collectTpmC();
//                log.info("Current AP Threads Number is: " + TPMonitor.apNumber);
                if (realTimeTPS < threshold) {
                    benchmark.oltp.OLTPClient.stopBecauseOfLowTP();
                    log.info("real time TPM: " + realTimeTPS);
                    log.info("Threshold TPM: " + threshold);
                    log.info("Current AP Threads Number is: " + TPMonitor.apNumber);
                    log.info("Time: " + System.nanoTime());
                    log.info("tpmC under threshold! Stop Benchmarking!");
                    systemNotStopRunning = false;
                }
                Thread.sleep(30000);     // 间隔30s统计一次
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public double collectTpmC() throws SQLException {
        return benchmark.oltp.OLTPClient.collectTpmC();
    }

    public static void addAPThread(int x) {
        apNumber += x;
    }
}
