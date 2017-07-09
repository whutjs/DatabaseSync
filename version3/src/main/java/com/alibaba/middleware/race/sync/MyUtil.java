package com.alibaba.middleware.race.sync;

/**
 * Created by Jenson on 2017/6/8.
 */
public class MyUtil {
//    /**
//     * 63|62       51|50     39|38|37  19|18    0
//     * +---------------------------------------+
//     * |0|first_name|last_name|sex|score|score2|
//     * +--------------------------------------+
//     */
//    // 12bit掩码
//    private static final long BITMASK_12 = 0xfff;
//    private static final long BITMASK_19 = 0x7ffff;
//
//    public static long extractScore2(long record) {
//        return (record & BITMASK_19);
//    }
//
//    private static final long SETSCORE2_MASK = (~BITMASK_19);
//    public static long setScore2(long record, long score2) {
//            record &= SETSCORE2_MASK;
//            return (record | score2);
//    }
//
//    public static long extractScore(long record) {
//        return ((record>>>19) & BITMASK_19);
//    }
//
//    private static final long SETSCORE_MASK = ~(BITMASK_19 << 19);
//    public static long setScore(long record, long score) {
//        record &= SETSCORE_MASK;
//        return (record | (score<<19));
//    }
//
//    public static long extractSex(long record) {
//        return ((record>>>38) & 1);
//    }
//
//    private static final long SETSEX_MASK = ~(1 << 38);
//    public static long setSex(long record, long sex) {
//        record &= SETSEX_MASK;
//        return (record | (sex<<38));
//    }
//
//    public static long extractLastName(long record) {
//        return ((record>>>39) & BITMASK_12);
//    }
//
//    private static final long SETLASTNAME_MASK = (~(BITMASK_12 << 39));
//    public static long setLastName(long record, long lastName) {
//        record &= SETLASTNAME_MASK;
//        return (record | (lastName<<39));
//    }
//
//    public static long extractFirstName(long record) {
//        return ((record>>>51) & BITMASK_12);
//    }
//
//    private static final long SETFIRSTNAME_MASK = (~(BITMASK_12 << 51));
//    public static long setFirstName(long record, long firstName) {
//        record &= SETFIRSTNAME_MASK;
//        return (record | (firstName<<51));
//    }

}