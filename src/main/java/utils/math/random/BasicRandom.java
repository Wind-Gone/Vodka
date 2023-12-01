package utils.math.random;/*
 * jTPCCUtil - utility functions for the Open Source Java implementation of
 *    the TPC-C benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * 
 *
 */


import java.util.*;
import java.util.Date;

public class BasicRandom {
    private static final char[] aStringChars = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final String[] cLastTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
    private static final String[] pNameColor = {"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"};
    private static final String[] containerSyllable1 = {"SM", "LG", "MED", "JUMBO", "WRAP"};
    private static final String[] containerSyllable2 = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};

    private static final String[] typeSyllable1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
    private static final String[] typeSyllable2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
    private static final String[] typeSyllable3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
    private static final String[] cktSegment = {"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static final String[] sMode = {"REG AIR", "AIR", "RAIL", "TRUCK", "MAIL", "FOB", "SHIP"};
    private static final String[] rFlag = {"R", "A", "N"};
    private static final String[] sInstruct = {"DELIVER IN PERSON", "COLLECT COD", "TAKE BACK RETURN", "NONE"};

    private static long nURandCLast;
    private static long nURandCC_ID;
    private static long nURandCI_ID;
    private static boolean initialized = false;

    private Random random;

    private String[] string_A_6;
    private String[] string_A_8;
    private String[] string_A_10;
    private String[] string_A_12;
    private String[] string_A_14;
    private String[] string_A_24;
    private String[] string_A_26;
    private String[] string_A_300;

    private String[] string_B_4;
    private String[] string_B_8;
    private String[] string_B_10;
    private String[] string_B_12;
    private String[] string_B_24;
    private String[] string_B_200;

    /*
     * jTPCCRandom()
     *
     *     Used to create the master jTPCCRandom() instance for loading
     *     the database. See below.
     */
    public BasicRandom() {
        if (initialized)
            throw new IllegalStateException("Global instance exists");

        this.random = new Random(System.nanoTime());
        BasicRandom.nURandCLast = nextLong(0, 255);
        BasicRandom.nURandCC_ID = nextLong(0, 1023);
        BasicRandom.nURandCI_ID = nextLong(0, 8191);

        initialized = true;
        System.out.println("utils.math.random initialized");
    }

    /*
     * jTPCCRandom(CLoad)
     *
     *     Used to create the master jTPCCRandom instance for running
     *     a benchmark utils.load.
     *
     *     TPC-C 2.1.6 defines the rules for picking the C values of
     *     the non-uniform utils.math.random number generator. In particular
     *     2.1.6.1 defines what numbers for the C value for generating
     *     C_LAST must be excluded from the possible range during run
     *     time, based on the number used during the utils.load.
     */
    public BasicRandom(long CLoad) {
        long delta;

        if (initialized)
            throw new IllegalStateException("Global instance exists");

        this.random = new Random(System.nanoTime());
        BasicRandom.nURandCC_ID = nextLong(0, 1023);
        BasicRandom.nURandCI_ID = nextLong(0, 8191);

        do {
            BasicRandom.nURandCLast = nextLong(0, 255);

            delta = Math.abs(BasicRandom.nURandCLast - CLoad);
            if (delta == 96 || delta == 112)
                continue;
            if (delta < 65 || delta > 119)
                continue;
            break;
        } while (true);

        initialized = true;
    }

    private BasicRandom(BasicRandom parent) {
        this.random = new Random(System.nanoTime());
        this.generateStrings();
    }

    /*
     * newRandom()
     *
     *     Creates a derived utils.math.random data generator to be used in another
     *     thread of the current benchmark utils.load or run process. As per
     *     TPC-C 2.1.6 all terminals during a run must use the same C
     *     values per field. The jTPCCRandom Class therefore cannot
     *     generate them per instance, but each thread's instance must
     *     inherit those numbers from a global instance.
     */
    public BasicRandom newRandom() {
        return new BasicRandom(this);
    }

    private void generateStrings() {
        string_A_6 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_6[i] = getAString(6, 6);
        string_A_8 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_8[i] = getAString(8, 8);
        string_A_10 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_10[i] = getAString(10, 10);
        string_A_12 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_12[i] = getAString(12, 12);
        string_A_14 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_14[i] = getAString(14, 14);
        string_A_24 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_24[i] = getAString(24, 24);
        string_A_26 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_26[i] = getAString(26, 26);
        string_A_300 = new String[100];
        for (int i = 0; i < 100; i++)
            string_A_300[i] = getAString(300, 300);

        string_B_4 = new String[50];
        for (int i = 0; i < 50; i++)
            string_B_4[i] = getAString(i / 10, i / 10);
        string_B_8 = new String[90];
        for (int i = 0; i < 90; i++)
            string_B_8[i] = getAString(i / 10, i / 10);
        string_B_10 = new String[110];
        for (int i = 0; i < 110; i++)
            string_B_10[i] = getAString(i / 10, i / 10);
        string_B_12 = new String[130];
        for (int i = 0; i < 130; i++)
            string_B_12[i] = getAString(i / 10, i / 10);
        string_B_24 = new String[250];
        for (int i = 0; i < 250; i++)
            string_B_24[i] = getAString(i / 10, i / 10);
        string_B_200 = new String[2010];
        for (int i = 0; i < 2010; i++)
            string_B_200[i] = getAString(i / 10, i / 10);
    }

    /*
     * nextDouble(x, y)
     *
     *     Produce double utils.math.random number uniformly distributed in [x .. y]
     */
    public double nextDouble(double x, double y) {
        return random.nextDouble() * (y - x) + x;
    }

    /*
     * nextLong(x, y)
     *
     *     Produce a utils.math.random number uniformly distributed in [x .. y]
     */
    public long nextLong(long x, long y) {
        return (long) (random.nextDouble() * (y - x + 1) + x);
    }

    /*
     * nextInt(x, y)
     *
     *     Produce a utils.math.random number uniformly distributed in [x .. y]
     */
    public int nextInt(int x, int y) {
        return (int) (random.nextDouble() * (y - x + 1) + x);
    }

    /*
     * getAString(x, y)
     *
     *     Procude a utils.math.random alphanumeric string of length [x .. y].
     *
     *     Note: TPC-C 4.3.2.2 asks for an "alhpanumeric" string.
     *     Comment 1 about the character set does NOT mean that this
     *     function must eventually produce 128 different characters,
     *     only that the "character set" used to store this data must
     *     be able to represent 128 different characters. '#@!%%ÄÖß'
     *     is not an alphanumeric string. We can save ourselves a lot
     *     of UTF8 related trouble by producing alphanumeric only
     *     instead of cartoon style curse-bubbles.
     */
    public String getAString(long x, long y) {
        StringBuilder result = new StringBuilder();
        long len = nextLong(x, y);
        long have = 1;

        if (y <= 0)
            return result.toString();

        result.append(aStringChars[(int) nextLong(0, 51)]);
        while (have < len) {
            result.append(aStringChars[(int) nextLong(0, 61)]);
            have++;
        }

        return result.toString();
    }

    public String getAString_6_10() {
        String result = "";

        result += string_A_6[nextInt(0, 99)];
        result += string_B_4[nextInt(0, 49)];

        return result;
    }

    public String getAString_8_16() {
        String result = "";

        result += string_A_8[nextInt(0, 99)];
        result += string_B_8[nextInt(0, 89)];

        return result;
    }

    public String getAString_10_20() {
        String result = "";

        result += string_A_10[nextInt(0, 99)];
        result += string_B_10[nextInt(0, 109)];

        return result;
    }

    public String getAString_12_24() {
        String result = "";

        result += string_A_12[nextInt(0, 99)];
        result += string_B_12[nextInt(0, 129)];

        return result;
    }

    public String getAString_14_24() {
        String result = "";

        result += string_A_14[nextInt(0, 99)];
        result += string_B_10[nextInt(0, 109)];

        return result;
    }

    public String getAString_24() {
        String result = "";

        result += string_A_24[nextInt(0, 99)];

        return result;
    }

    public String getAString_26_50() {
        String result = "";

        result += string_A_26[nextInt(0, 99)];
        result += string_B_24[nextInt(0, 249)];

        return result;
    }

    public String getAString_300_500() {
        String result = "";

        result += string_A_300[nextInt(0, 99)];
        result += string_B_200[nextInt(0, 2009)];

        return result;
    }

    /*
     * getNString(x, y)
     *
     *     Produce a utils.math.random numeric string of length [x .. y].
     */
    public String getNString(long x, long y) {
        StringBuilder result = new StringBuilder();
        long len = nextLong(x, y);
        long have = 0;

        while (have < len) {
            result.append((char) (nextLong('0', '9')));
            have++;
        }

        return result.toString();
    }

    /*
     * getItemID()
     *
     *     Produce a non uniform utils.math.random Item ID.
     */
    public int getItemID() {
        return (int) ((((nextLong(0, 8191) | nextLong(1, 100000)) + nURandCI_ID)
                % 100000) + 1);
    }

    /*
     * getCustomerID()
     *
     *     Produce a non uniform utils.math.random Customer ID.
     */
    public int getCustomerID() {
        return (int) ((((nextLong(0, 1023) | nextLong(1, 3000)) + nURandCC_ID)
                % 3000) + 1);
    }

    /*
     * getCLast(num)
     *
     *     Produce the syllable representation for C_LAST of [0 .. 999]
     */
    public String getCLast(int num) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            result.insert(0, cLastTokens[num % 10]);
            num /= 10;
        }
        return result.toString();
    }

    /*
     * getCLast()
     *
     *     Procude a non uniform utils.math.random Customer Last Name.
     */
    public String getCLast() {
        long num;
        num = (((nextLong(0, 255) | nextLong(0, 999)) + nURandCLast) % 1000);
        return getCLast((int) num);
    }

    public String getState() {
        String result = "";

        result += (char) nextInt('A', 'Z');
        result += (char) nextInt('A', 'Z');

        return result;
    }


    /*
     * Methods to retrieve the C values used.
     */
    public long getNURandCLast() {
        return nURandCLast;
    }

    public long getNURandCC_ID() {
        return nURandCC_ID;
    }

    public long getNURandCI_ID() {
        return nURandCI_ID;
    }

    public int differentDaysByMillisecond(Date date1, Date date2) {//计算两个日期相差天数
        return Math.abs((int) ((date2.getTime() - date1.getTime()) / (1000 * 3600 * 24)));
    }

    public Date getDateAfter(Date d, int day) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
        now.add(Calendar.DATE, day);
        return now.getTime();
    }

    public String getPName(int low, int high) {
        String result;
        int num = nextInt(low, high);
        result = pNameColor[num];
        return result;
    }

    public String getContainer() {
        String result = "";
        result += containerSyllable1[nextInt(0, 4)];
        result += " ";
        result += containerSyllable2[nextInt(0, 7)];
        return result;
    }

    public String getSmode(int low, int high) {
        return sMode[nextInt(low, high)];
    }

    public String getSinstruct(int low, int high) {
        return sInstruct[nextInt(low, high)];
    }

    public String getType() {
        String result = "";
        result += typeSyllable1[nextInt(0, 5)];
        result += " ";
        result += typeSyllable2[nextInt(0, 4)];
        result += " ";
        result += typeSyllable3[nextInt(0, 4)];
        return result;
    }

    public String getCktSegement(int low, int high) {
        String result = "";
        result += cktSegment[nextInt(low, high)];
        return result;
    }
} // end jTPCCRandom
