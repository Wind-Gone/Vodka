package config;

import lombok.Getter;
import lombok.Setter;
import utils.common.CheckParamUtil;

import java.io.IOException;

@Getter
@Setter
public class RunningProperty extends Property {

    // scale factor(data size, workload size, cluster scale and running time)
    private int warehouses;
    private int TPterminals;
    private int runTxnsPerTerminal;
    private double runMins;
    private int limitTxnsPerMin;
    private double TPthreshold;
    private int APTerminals;
    private int testTimeInterval;
    private int dynamicParam;
    private boolean terminalWarehouseFixed;

    // transaction mixture rate
    private double newOrderWeight;
    private double paymentWeight;
    private double orderStatusWeight;
    private double deliveryWeight;
    private double stockLevelWeight;
    private double receiveGoodsWeight;

    // stored procedures
    private boolean useStoredProcedures;

    public boolean isHtapCheck() {
        return isHtapCheck;
    }

    // htap check
    private boolean isHtapCheck;
    private String htapCheckType;
    private String htapCheckCrossQuantity;
    private String htapCheckCrossFrequency;
    private String htapCheckApNum;
    private String htapCheckConnAp;
    private String htapCheckConnTp;
    private String htapCheckGapTime;
    private int htapCheckFreshnessDataBound;
    private int htapCheckQueryNumber;
    private String htapCheckFreshLagThreshold;


    // result directory
    private String resultDirectory;

    // operating system information collector
    private boolean osCollector;
    private String osCollectorScript;
    private int osCollectorInterval;
    private String osCollectorSSHAddr;
    private String osCollectorDevices;

    // data file
    private boolean writeCSV;
    private int loadWorkers;

    // parallel
    private boolean parallel;
    private int parallel_degree;

    // isolation level
    private int isolation_level;

    // dynamic ap
    private boolean checkInterference;
    private int step;
    private int increaseInterval;

    // weak read
    private boolean isWeakRead;
    private int weakReadTime;

    public RunningProperty(String path) throws IOException {
        super(path);
    }

    private void checkProperty() throws Exception {
        // check numeric properties with CheckParamsUtil
        CheckParamUtil.checkNonNegativeOrZero(warehouses, "warehouses");
        CheckParamUtil.checkNonNegativeOrZero(TPterminals, "TPterminals");
        CheckParamUtil.checkNonNegative(APTerminals, "APterminals");
        CheckParamUtil.checkSumEqualOne(newOrderWeight, paymentWeight, orderStatusWeight, deliveryWeight, stockLevelWeight, receiveGoodsWeight);
        CheckParamUtil.checkMutualExclusive(runTxnsPerTerminal, "runTxnsPerTerminal", (int) runMins, "runMins");
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckType, "htapCheckType");
        CheckParamUtil.checkLowerEqualthanValue(htapCheckType, "htapCheckType", 3);
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckCrossQuantity, "htapCheckCrossQuantity");
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckCrossFrequency, "htapCheckCrossFrequency");
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckConnAp, "htapCheckConnAp");
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckConnTp, "htapCheckConnTp");
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckGapTime, "htapCheckGapTime");
        CheckParamUtil.checkFormalExistsLatterNull(isHtapCheck, "isHtapCheck", htapCheckFreshLagThreshold, "htapCheckFreshLagThreshold");
        CheckParamUtil.checkNonNegativeOrZero(htapCheckFreshnessDataBound, "htapCheckFreshnessDataBound");
        CheckParamUtil.checkNonNegativeOrZero(htapCheckQueryNumber, "htapCheckQueryNumber");
        CheckParamUtil.checkNonNegativeOrZero(step, "step");
        CheckParamUtil.checkNonNegativeOrZero(increaseInterval, "increaseInterval");
    }

    @Override
    public void loadProperty() {

        try {
            // scale factor
            warehouses = Integer.parseInt(props.getProperty("warehouses", "100"));
            TPterminals = Integer.parseInt(props.getProperty("TPterminals", "60"));
            APTerminals = Integer.parseInt(props.getProperty("APTerminals", "1"));
            runMins = Double.parseDouble(props.getProperty("runMins", "5"));
            runTxnsPerTerminal = Integer.parseInt(props.getProperty("runTxnsPerTerminal", "100"));
            limitTxnsPerMin = Integer.parseInt(props.getProperty("limitTxnsPerMin", "10000"));
            TPthreshold = Double.parseDouble(props.getProperty("TPthreshold", "0,1"));
            testTimeInterval = Integer.parseInt(props.getProperty("testTimeInterval", "30"));
            dynamicParam = Integer.parseInt(props.getProperty("dynamicParam", "1"));
            terminalWarehouseFixed = Boolean.parseBoolean(props.getProperty("terminalWarehouseFixed", "true"));

            // transaction rate
            newOrderWeight = Double.parseDouble(props.getProperty("newOrderWeight", "44"));
            paymentWeight = Double.parseDouble(props.getProperty("paymentWeight", "42"));
            orderStatusWeight = Double.parseDouble(props.getProperty("orderStatusWeight", "4"));
            deliveryWeight = Double.parseDouble(props.getProperty("deliveryWeight", "4"));
            stockLevelWeight = Double.parseDouble(props.getProperty("stockLevelWeight", "4"));
            receiveGoodsWeight = Double.parseDouble(props.getProperty("receiveGoodsWeight", "2"));
            useStoredProcedures = Boolean.parseBoolean(props.getProperty("useStoredProcedures", "false"));

            // htap check
            isHtapCheck = Boolean.parseBoolean(props.getProperty("isHtapCheck", "true"));
            htapCheckType = props.getProperty("htapCheckType", "1");
            htapCheckCrossQuantity = props.getProperty("htapCheckCrossQuantity", "100");
            htapCheckCrossFrequency = props.getProperty("htapCheckCrossFrequency", "100");
            htapCheckApNum = props.getProperty("htapCheckApNum", "10");
            htapCheckConnAp = props.getProperty("htapCheckConnAp", "jdbc:postgresql://49.52.27.35:5532/benchmarksql");
            htapCheckConnTp = props.getProperty("htapCheckConnTp", "jdbc:postgresql://49.52.27.33:5532/benchmarksql");
            htapCheckFreshLagThreshold = props.getProperty("htapCheckFreshLagThreshold", "10,100");
            htapCheckQueryNumber = Integer.parseInt(props.getProperty("htapCheckQueryNumber", "100"));
            htapCheckFreshnessDataBound = Integer.parseInt(props.getProperty("htapCheckFreshnessDataBound", "1000"));
            // htapCheckGapTime = props.getProperty("htapCheckGapTime", 0);

            // result directory
            resultDirectory = props.getProperty("resultDirectory", "results/my_result_%tY-%tm-%td_%tH%tM%tS");

            // operating system information collector
            osCollectorScript = props.getProperty("osCollectorScript", null);
            osCollectorInterval = Integer.parseInt(props.getProperty("osCollectorInterval", "10"));
            osCollectorSSHAddr = props.getProperty("osCollectorSSHAddr", null);
            osCollectorDevices = props.getProperty("osCollectorDevices", "net_eth0,blk_vdb");

            // data file
            loadWorkers = Integer.parseInt(props.getProperty("loadWorkers", "200"));

            // parallel processing
            parallel = Boolean.parseBoolean(props.getProperty("parallel", "false"));
            parallel_degree = Integer.parseInt(props.getProperty("parallel_degree", "8"));

            // isolation level
            isolation_level = Integer.parseInt(props.getProperty("isolation_level", "2"));

            // dynamic ap
            checkInterference = Boolean.parseBoolean(props.getProperty("checkInterference", "false"));
            step = Integer.parseInt(props.getProperty("step", "1"));
            increaseInterval = Integer.parseInt(props.getProperty("increaseInterval", "1"));

            // weak read
            isWeakRead = Boolean.parseBoolean(props.getProperty("weak_read", "false"));
            weakReadTime = Integer.parseInt(props.getProperty("weak_read_time", "5"));

            // check the validity of each parameter
            checkProperty();
        } catch (Exception e) {
            e.printStackTrace();
        }

    } // end loadProperty
}
