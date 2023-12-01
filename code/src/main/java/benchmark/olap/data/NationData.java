package benchmark.olap.data;

public class NationData {
    public static Integer[] nationKey = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
    public static String[] nationName = {"ALGERIA", "ARFENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA",
            "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JODAN", "KENYA", "MORACCO", "MOZAMBIQUE", "PERU",
            "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"};
    public static Integer[] n_regionKey = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};

    public static void main(String[] args) {
        System.out.println(NationData.nationKey.length == NationData.n_regionKey.length && NationData.n_regionKey.length == NationData.nationName.length);
    }
}
