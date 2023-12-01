package utils.common;

import org.apache.log4j.Logger;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class DynamicAP implements Runnable {
    private static org.apache.log4j.Logger log = Logger.getLogger(DynamicAP.class);

    String iConn;
    Properties dbProps;
    int dbType;
    int testTimeInterval;
    OLTPClient parent;
    int dynamicParam;
    boolean parallelSwitch;
    int isolation_level;
    int parallel_degree;
    String resultDirName;

    int step;
    int increaseInterval;
    private ArrayList<OLAPTerminal> APterminals;

    public DynamicAP(String iConn, Properties dbProps, int dbType, int testTimeInterval, OLTPClient parent,
            int dynamicParam,
            boolean parallelSwitch, int isolation_level, int parallel_degree, String resultDirName, int step,
            int increaseInterval) {
        this.iConn = iConn;
        this.dbProps = dbProps;
        this.dbType = dbType;
        this.testTimeInterval = testTimeInterval;
        this.parent = parent;
        this.dynamicParam = dynamicParam;
        this.parallelSwitch = parallelSwitch;
        this.isolation_level = isolation_level;
        this.parallel_degree = parallel_degree;
        this.resultDirName = resultDirName;

        this.step = step;
        this.increaseInterval = increaseInterval;
    }

    @Override
    public void run() {
        APterminals = new ArrayList<>();

        try {
            Thread.sleep(120000); // warmup2分钟
            while (true) {
                if (benchmark.oltp.OLTPClient.getSignalTerminalsRequestEndSent())
                    break;

                log.info("add " + step + " AP Terminals...");

                for (int i = 0; i < step; i++) {
                    OLAPTerminal APTerminal = new OLAPTerminal(iConn, dbProps, dbType, testTimeInterval, parent,
                            dynamicParam, parallelSwitch, isolation_level, parallel_degree, resultDirName);
                    (new Thread(APTerminal)).start();
                    APterminals.add(APTerminal);
                }

                TPMonitor.addAPThread(step);

                Thread.sleep(increaseInterval * 1000); // 间隔 increaseInterval 秒增加 step 个AP线程
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (OLAPTerminal tOlapTerminal : APterminals)
            if (tOlapTerminal != null)
                tOlapTerminal.stopRunningWhenPossible();
    }
}
