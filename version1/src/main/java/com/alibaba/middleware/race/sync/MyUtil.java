package com.alibaba.middleware.race.sync;

/**
 * Created by Jenson on 2017/6/8.
 */
public class MyUtil {
    private static final int OFFSET_MARK = ~7;
    // 文件编号的偏移FILENO_OFFSET
    private static final int FILENO_OFFSET = 28;
    /** 用int 32bit来表示文件编号+文件偏移：
     *  用末31位（2G）表示文件偏移
     *  用高4位表示文件序号
     */

    /**
     *
     * @param metaData 待设置的metaData
     * @param offset 偏移
     * @return 设置后的metaData
     */
    public static int setOffsetMetaData(int metaData, int offset) {
            offset = offset & (~7);
            offset >>>= 3;
            return (metaData | offset);
    }

    public static int getOffset(int metaData) {
        return ((metaData & 0x0FFFFFFF) << 3);
    }

    /**
     * 设置metaData的文件编号属性
     * @param metaData
     * @param fileNO
     * @return
     */
    public static int setFileNOMetaData(int metaData, int fileNO) {
            return (metaData | (fileNO << FILENO_OFFSET));
    }

    /**
     * 从metaData中提取出fileNO
     * @param metaData
     * @return
     */
    public static int getFileNO(int metaData) {
        return (metaData >>> FILENO_OFFSET);
    }
}
