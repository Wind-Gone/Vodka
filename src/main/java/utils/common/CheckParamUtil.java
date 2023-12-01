package utils.common;

import org.apache.log4j.Logger;

import java.util.Arrays;

public class CheckParamUtil {
    private static org.apache.log4j.Logger log = Logger.getLogger(CheckParamUtil.class);

    public static void checkEqual(double target, double actual, String name) {
        if (target != actual) {
            throw new RuntimeException(name + " should equals to " + target + ", check input '" + name + "': " + actual);
        }
    }

    public static void checkEqual(String target, String actual, String name) {
        if (target.equals(actual)) {
            throw new RuntimeException(name + " should equals to " + target + ", check input '" + name + "': " + actual);
        }
    }

    public static void checkPositive(double param, String name) {
        if (param < 1) {
            throw new RuntimeException(name + " should be positive double, check input '" + name + "': " + param);
        }
    }

    public static void checkNonNegativeOrZero(double param, String name) {
        if (param <= 0) {
            throw new RuntimeException(name + " should be positive double, check input '" + name + "': " + param);
        }
    }

    public static void checkNonNegative(double param, String name) {
        if (param < 0) {
            throw new RuntimeException(name + " should be non-negative integer, check input '" + name + "': " + param);
        }
    }

    public static void checkRange(double param, String name, double leftRange, double rightRange) {
        if (param < leftRange || param > rightRange) {
            throw new RuntimeException(name + " should among [" + leftRange + "," + rightRange + "], check input '"
                    + name + "': " + param);
        }
    }

    public static void checkTerminalRange(int leftRange, int rightRange, int warehouses) {
        checkRange(leftRange, "terminalRange(left)", 1, warehouses);
        checkRange(rightRange, "terminalRange(right)", 1, warehouses);
        if (leftRange > rightRange) {
            throw new RuntimeException("illegal terminal range inequality, check input 'terminalRange': "
                    + leftRange + "," + rightRange);
        }
    }

    public static void checkMixtureTransactions(int... weights) {
        if (weights.length != 9) {
            throw new RuntimeException("number of transaction type shoud be 9, check input transaction mixture " + Arrays.toString(weights));
        }
        int totalWeight = 0;
        for (int weight : weights) {
            totalWeight += weight;
        }
        if (totalWeight != 100) {
            throw new RuntimeException("get unexpected mixture transaction rate, check input 'newOrderWeight': "
                    + weights[0] + ", 'paymentWeight': " + weights[1] + ", 'orderStatusWeight': "
                    + weights[2] + ", 'deliveryWeight': " + weights[3] + ", 'stockLevelWeight': "
                    + weights[4] + ", 'updateItemWeight': " + weights[5] + ", 'updateStockWeight': "
                    + weights[6] + ", 'globalSnapshotWeight': " + weights[7]
                    + ", 'globalDeadlockWeight': " + weights[8]);
        }
    }

    public static void checkNull(String str, String name) {
        if (str.equals("")) {
            throw new RuntimeException("can not get requisite property, check input '" + name + "'");
        }
    }

    public static void checkMutualExclusive(double a, String name1, int b, String name2) {
        if (a == 0 && b != 0) {
            log.info("Vodka-DBHammer, " + name2 + " is: " + b);
        } else if (a != 0 && b == 0) {
            log.info("Vodka-DBHammer, " + name1 + " is: " + a);
        } else {
            throw new RuntimeException("Error happens in two mutual exclusive variables in " + name1 + " : " + a + ", " + name2 + " is: " + b);
        }
    }

    public static void checkGreater(int left, int right) throws Exception {
        if (left > right) {
            throw new Exception("left cannot be greater " + "than the right ");
        }
    }

    public static void checkSumEqualOne(double newOrderWeight, double paymentWeight, double orderStatusWeight, double deliveryWeight, double stockLevelWeight, double receiveGoodsWeight) throws Exception {
        checkNonNegative(newOrderWeight, "newOrderWeight");
        checkNonNegative(paymentWeight, "paymentWeight");
        checkNonNegative(orderStatusWeight, "orderStatusWeight");
        checkNonNegative(deliveryWeight, "deliveryWeight");
        checkNonNegative(stockLevelWeight, "stockLevelWeight");
        checkNonNegative(receiveGoodsWeight, "receiveGoodsWeight");
        double sumWeight = newOrderWeight + paymentWeight + orderStatusWeight + deliveryWeight + stockLevelWeight + receiveGoodsWeight;
        sumWeight = Math.round(sumWeight * 100.0) / 100.0;
        if (sumWeight != 100.0) {
            throw new Exception("Sum of mix percentage parameters must equal 100%! Have %f" + sumWeight);
        }
    }

    public static void checkFormalExistsLatterNull(boolean flag, String name1, String latter, String name2) throws Exception {
        if (flag && latter == null) {
            throw new Exception("When the " + name1 + " is true, the " + name2 + " cannot be null.");
        }
    }

    public static void checkLowerEqualthanValue(String a, String name, int i) throws Exception {
        if (Integer.parseInt(a) > i) {
            throw new Exception("The parameter " + name + " cannot greater than  " + i + ".");
        }
    }
}
