package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

// 我也不知道起什么名字好...
class PkUpdateRecord {
    //// 变更前主键，作为pkUpdateNotInRange集合的主键
    //public Long pkBeforeModify;
    // 变更类型
    public byte modifyType;
    // 变更后主键。该键用于索引pkRecordMap
    public Long pkAfterModify;

//    @Override
//    public int hashCode() {
//        return pkBeforeModify.hashCode();
//    }
//
//    @Override
//    public boolean equals(Object arg0) {
//        PkUpdateRecord obj=(PkUpdateRecord)arg0;
//        return (this.pkBeforeModify.longValue() == obj.pkBeforeModify.longValue());
//    }
}
/**
 * 用于多线程构建索引
 * Created by Jenson on 2017/6/10.
 */
public class Worker implements Runnable{
    Logger logger = LoggerFactory.getLogger(Worker.class);
    // 线程号，方便打log之类
    private final int threadNO;
    // 文件编号
    private final int curFileNo;
    private final CountDownLatch latch;

    /* TODO:用于探索数据特征 */
    private int insertCnt = 0;
    private int updateCnt = 0;
    private int deleteCnt = 0;
    private int pkUpdateCnt = 0;
    // 对于保存offset的meta数组的最少元素数目
    private int minMetaArrayNum = 10000000;
    private int maxMetaArrayNum = -1;
    private int totalMetaArrayNum = 0;

    /** 如果一个主键在当前线程的范围内没有被插入就被更新了，
     * 那么需要在pkUpdateNotInRange记录变更前的主键和变更后的；
     * 同时依旧在pkRecordMap插入变更后的主键的变更操作的offset;
     * 然后在pkRecordReverseMap记录：变更后的主键->变更前主键的映射，
     * 方便后面主键再次变更时，修改pkUpdateNotInRange。
     * 对于Delete变更，只需在pkUpdateNotInRange记录即可。
     */
    // 自己需要构建的那一部分主键->该键变更记录的offset
    private HashMap<Long, int[]> pkRecordMap;
    // 不在该线程的范围内进行主键更新的映射记录
    private HashMap<Long, PkUpdateRecord> pkUpdateNotInRange;
    private HashMap<Long, Long> pkRecordReverseMap;

    // 用于读数据
    private final ByteBuffer mapFile;
    // 表示要处理的schema和table
    private final String schema;
    private final String tableName;
    // 该worker所读文件的起始偏移
    private final int startOffset;
    // pkRecordMap的int[]的初始大小，需要根据数据量做调整
    private final int metaArrayInitSize = 6;
    // 用来存需要提取出字符串的bytes
    private final byte[] strBytes = new byte[128];

    public Worker(final int tno, final int fno, final int startOff, final String schemaName, final String table,
                  final HashMap<Long, int[]> pkMap, final HashMap<Long, PkUpdateRecord> pkUpdateSet,
                  final ByteBuffer mapFile, final CountDownLatch lt) {
        this.threadNO = tno;
        this.curFileNo = fno;
        this.startOffset = startOff;
        this.schema = schemaName;
        this.tableName = table;
        this.pkRecordMap = pkMap;
        this.pkUpdateNotInRange = pkUpdateSet;
        this.mapFile = mapFile;
        this.pkRecordReverseMap = new HashMap<>();
        this.latch = lt;
    }

    @Override
    public void run() {
        startParseFile();
        this.latch.countDown();
        // 统计下每个meta数组装了多少元素
        for(final Long pk : this.pkRecordMap.keySet()) {
            int[] metaArray = this.pkRecordMap.get(pk);
            int num = metaArray[0]-1;
            if(num > this.maxMetaArrayNum) {
                this.maxMetaArrayNum = num;
            }
            if(num < this.minMetaArrayNum) {
                this.minMetaArrayNum = num;
            }
            this.totalMetaArrayNum += num;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Worker").append(this.threadNO).append(" finished. pkRecordMap size:")
                .append(pkRecordMap.size()).append(" pkUpdateNotInRange size:").append(pkUpdateNotInRange.size())
                .append(" pkRecordReverseMap size:").append(pkRecordReverseMap.size())
                .append(" insertCnt:").append(insertCnt).append(" updateCnt:").append(updateCnt)
                .append(" deleteCnt:").append(deleteCnt).append(" pkUpdateCnt:").append(pkUpdateCnt)
                .append(" minMetaArrayNum:").append(minMetaArrayNum).append(" maxMetaArrayNum:").append(maxMetaArrayNum)
        .append(" totalMetaArrayNum:").append(totalMetaArrayNum);
        logger.info(sb.toString());
    }

    public void releaseResource() {
        this.pkRecordReverseMap.clear();
        this.pkRecordReverseMap = null;
        // 下面两个是从主线程传进来的，置null即可
        this.pkUpdateNotInRange = null;
        this.pkRecordMap = null;
    }

    private void startParseFile() {
        // 用于计算'|'的数目
        int verticalLineCnt = 0;
        int prePos = 0, newPos = 0, len = 0;
        while(this.mapFile.hasRemaining()) {
            verticalLineCnt = 0;
            // 开始解析
            // 这就是当前变更记录的起始位置
            prePos = this.mapFile.position();
            // 读了3个'|'后就是schema的第一个字符
            while(this.mapFile.hasRemaining()) {
                byte b = this.mapFile.get();
                if(b == '|') {
                    if(++verticalLineCnt >= 3) {
                        break;
                    }
                }
            }
            // 当前变更记录实际应该存的有效偏移,因为要把schema前面的'|'也算上，所以是-1
            final int recordOffset = this.mapFile.position()-1 + this.startOffset;
            // 当前mapFile指向schema的第一个字符
            final String schemaName = getStrEndWithVerticalLine(this.mapFile);
            //logger.info("schemaName="+schemaName);
            // NOTE:读完之后，mapFile再次指向的是schema后面的'|'!!!!
            // 是否是要查询的schema
            boolean isSchema = false;
            if(schemaName != null && schemaName.length() == this.schema.length()
                    && this.schema.equals(schemaName)) {
                isSchema = true;
            }
            if(!isSchema) {
                // 如果不是，直接skip到下一行变更记录。
                // 找到'\n'
                while(this.mapFile.hasRemaining()) {
                    if(this.mapFile.get() == '\n') {
                        break;
                    }
                }
                // 现在mapFile指向下一条变更记录的第一个字符'|'
                continue;
            }
            // 是当前的schema。继续提取出tableName
            // mapFile当前指向的是schema后面的'|'。skip掉
            if(this.mapFile.get() != '|') {
                logger.info("Error in Thread:"+this.threadNO+" mapFile解析错误！！应该指向schema后的'|'!");
                return;
            }
            final String table = getStrEndWithVerticalLine(this.mapFile);
            //logger.info("table="+table);
            boolean isTable = false;
            if(table != null && table.length() == this.tableName.length()
                    && this.tableName.equals(table)) {
                isTable = true;
            }
            if(!isTable) {
                // 不是，skip到下一条
                // 找到'\n'
                while(this.mapFile.hasRemaining()) {
                    if(this.mapFile.get() == '\n') {
                        break;
                    }
                }
                // 现在mapFile指向下一条变更记录的第一个字符'|'
                continue;
            }
            // 现在mapFile指向table的下一个'|'
            if(this.mapFile.get() != '|') {
                logger.info("Error in Thread:"+this.threadNO+" mapFile解析错误！！应该指向table后的'|'!");
                return;
            }
            // 下一个字符就是'I U D'了
            final byte modifiedType = this.mapFile.get();
            // 读掉后面的'|'
            this.mapFile.get();
            if(modifiedType == 'I') {
                parseInsert(this.mapFile, recordOffset);
            }else if(modifiedType == 'U') {
                parseUpdate(this.mapFile, recordOffset);
            }else if(modifiedType == 'D') {
                parseDelete(this.mapFile, recordOffset);
            }else{
                logger.info("Error in Thread:"+this.threadNO+" modify type:"+modifiedType);
            }
        }
    }

    /**
     * 提取出根据'|'分隔的字符串。dataBuf的当前位置应该指向有效的第一个字符，应该以'|'结尾
     * 返回之后，dataBuf指向的是末尾的'|'
     * @param dataBuf
     * @return
     */
    private String getStrEndWithVerticalLine(final ByteBuffer dataBuf) {
        // 当前第一个字符的起始位置
        int prePos = dataBuf.position();
        while(dataBuf.hasRemaining()) {
            if(dataBuf.get() == '|') {
                break;
            }
        }
        // 现在dataBuf指向的是'|'的下一个位置
        // newPos指向字符串后面的'|'的位置
        int newPos = dataBuf.position()-1;
        // 那么字符串的长度就是(newPos-prePos)
        int len = (newPos - prePos);
        if(len <= 0) {
            return "";
        }
        // NOTE:注意这里要让dataBuf的位置恢复到字符串第一个字符的位置！！
        dataBuf.position(prePos);
        if(len > strBytes.length ) {
            byte[] tmpBytes = new byte[len];
            dataBuf.get(tmpBytes);
            String longStr = new String(tmpBytes);
            logger.info("Found long str in Thread:"+this.threadNO+" len="+len + " str:"+longStr);
            return longStr;
        }else {
            // NOTE:读完之后，dataBuf再次指向的是schema后面的'|'!!!!
            dataBuf.get(strBytes, 0, len);
            return new String(strBytes, 0, len);
        }
    }

    /**
     * dataBuf当前位置应指向有效内容的第一个字符。Insert和Update只需要找到主键就可以了
     * @param dataBuf
     * @param recordOffset  当前变更记录应该保存的offset
     */
    private void parseInsert(final ByteBuffer dataBuf, final int recordOffset) {
        ++this.insertCnt;
        parseColumnInfo(dataBuf, recordOffset, Constants.INSERT_TYPE);
    }

    /**
     * dataBuf当前位置应指向有效内容的第一个字符。Insert和Update只需要找到主键就可以了
     * @param dataBuf
     * @param recordOffset  当前变更记录应该保存的offset
     */
    private void parseUpdate(final ByteBuffer dataBuf, final int recordOffset) {
        ++this.updateCnt;
        parseColumnInfo(dataBuf, recordOffset, Constants.UPDATE_TYPE);
    }

    /**
     * dataBuf当前位置应指向有效内容的第一个字符
     * @param dataBuf
     * @param recordOffset  当前变更记录应该保存的offset
     */
    private void parseDelete(final ByteBuffer dataBuf, final int recordOffset) {
        ++this.deleteCnt;
        parseColumnInfo(dataBuf, recordOffset, Constants.DELETE_TYPE);
    }

    /**
     * 增长并复制数组
     * @param oldArray
     * @return 新的数组
     */
    private int[] growAndCopyArray(final int[] oldArray) {
        final int nxtIdx = oldArray[0];
        // 满了,动态增长
        int[] newMetaArray = null;
        if(oldArray.length < this.metaArrayInitSize*2) {
            int newLen = oldArray.length*2;
            // 翻倍
            newMetaArray = new int[newLen];
        }else{
            // 增长一半
            int newLen = oldArray.length + oldArray.length/2;
            newMetaArray = new int[newLen];
        }
        // 复制过去
        for(int i = 0; i < nxtIdx; ++i) {
            newMetaArray[i] = oldArray[i];
        }
        return newMetaArray;
    }

    /**
     * 只需要解析Insert和Update两种变更信息
     * @param dataBuf dataBuf当前位置应指向有效内容的第一个字符
     * @param recordOffset
     * @param modifiedType
     */
    private void parseColumnInfo(final ByteBuffer dataBuf, final int recordOffset, final byte modifiedType) {
        // 格式：列信息|变更前列值|变更后列值|列信息|变更前列值|变更后列值|......
        // 提取出Column的属性
        int preIdx = dataBuf.position(), idx = dataBuf.position();
        while(dataBuf.hasRemaining()) {
            // dataBuf当前位置需要指向列的第一个有效字符，不能是'|'
            String columnInfo = getStrEndWithVerticalLine(dataBuf);
            //logger.info("columnInfo="+columnInfo);
            preIdx = 0;
            idx = columnInfo.indexOf(':');
            // 提取出列名
            final String columnName = columnInfo.substring(preIdx, idx);
            // idx指向的是':'，所以要前进一位
            final char valueType = columnInfo.charAt(++idx);
            // skip ':'
            ++idx;
            final char isPK = columnInfo.charAt(++idx);
            // 现在dataBuf的位置是'|'，前进一位
            dataBuf.get();
            // 读取出变更前列值
            final String columnValueBefore = getStrEndWithVerticalLine(dataBuf);
            // 现在dataBuf的位置是'|'，前进一位
            dataBuf.get();
            // 读取出变更后列值
            final String columnValueAfter = getStrEndWithVerticalLine(dataBuf);
            if(isPK == '1') {
                if(modifiedType == Constants.DELETE_TYPE) {
                    // 删除变更只有变更前列值
                    Long pk = Long.parseLong(columnValueBefore);
                    // 如果在当前线程的范围内没有这个主键，直接保存到pkUpdateNotInRange
                    int[] oldVal = this.pkRecordMap.remove(pk);
                    if(oldVal == null) {
                        // 没有，保存到pkUpdateNotInRange
                        PkUpdateRecord pur = new PkUpdateRecord();
                        pur.modifyType = Constants.DELETE_TYPE;
                        //pur.pkBeforeModify = pk;
                        this.pkUpdateNotInRange.put(pk, pur);
                    }else{
                        // 有的话，要先查询看之前有没有在当前线程范围内变更过key，
                        // 如果变更过需要使用第一次变更前的key来保存信息
                        Long firstKey = this.pkRecordReverseMap.get(pk);
                        if(firstKey != null) {
                            // 而且要修改pkUpdateNotInRange之前保存的信息！
                            this.pkUpdateNotInRange.get(firstKey).modifyType = Constants.DELETE_TYPE;
                        }else{
                            // 没有，直接保存
                            PkUpdateRecord pur = new PkUpdateRecord();
                            pur.modifyType = Constants.DELETE_TYPE;
                            //pur.pkBeforeModify = pk;
                            this.pkUpdateNotInRange.put(pk, pur);
                        }
                    }
                }else if(modifiedType == Constants.INSERT_TYPE) {
                    // 插入变更只有变更后列值
                    Long pk = Long.parseLong(columnValueAfter);
                    if(!this.pkRecordMap.containsKey(pk)) {
                        // 插入操作不用管pkUpdateNotInRange，直接插入即可
                        // TODO:优化：如果是查询范围内的key，可能变更记录会更多，其它
                        // TODO:不是范围内的key变更记录会少，可以少分配点
                        int[] recordMetaArray = new int[this.metaArrayInitSize];
                        // 数组的第一个元素存的是数组下一个空闲的位置（也就是当前元素个数）。
                        // 因此要注意数组有效内容是从1开始的
                        recordMetaArray[0] = 1;
                        this.pkRecordMap.put(pk, recordMetaArray);
                        recordMetaArray = null;
                    }
                    // 用int来记录metaData（文件编号+偏移）
                    int metaData = MyUtil.setOffsetMetaData(0, recordOffset);
                    metaData = MyUtil.setFileNOMetaData(metaData, this.curFileNo);
                    int[] metaArray = this.pkRecordMap.get(pk);
                    addMetaData2RecordMap(metaArray, metaData, pk, false);
                } else if(modifiedType == Constants.UPDATE_TYPE) {
                    /*
                        更新操作要考虑的因素较多：
                        1，如果要更新的键不在当前线程的pkRecordMap中，那么：
                            (1),如果主键也发生了变更，那么需要在pkUpdateNotInRange中也保存主键的
                            变更记录，然后在pkRecordMap中插入变更后的主键；在pkRecordReverseMap中保存
                            对应的pkRecordMap->pkUpdateNotInRange的映射；
                            (2),如果主键没有发生变更,只在pkRecordMap中插入对应主键的变更记录即可；
                        2,如果要更新的主键在当前线程的pkRecordMap中，那么：
                            (1),如果主键也发生了变更：
                                (a),如果pkUpdateNotInRange中没有该主键的记录，说明已存在的主键是通过1.(2) 在pkRecordMap插入的
                                记录，那么在pkUpdateNotInRange中插入相关记录，其中pkBeforeModify的值是发生变更前的pk值，
                                pkBeforeModify是发生变更后的值；然后修改pkRecordMap中对应的键并保存变更记录；在pkRecordReverseMap中保存
                                对应的pkRecordMap->pkUpdateNotInRange的映射；
                                (b),如果pkUpdateNotInRange中有该主键的记录（根据pkRecordReverseMap查询),说明是通过1.(1)插入的主键，
                                那么修改pkUpdateNotInRange的pkAfterModify的值，然后修改pkRecordMap并保存变更记录，并更新
                                pkRecordReverseMap映射；
                            (2)，如果主键没有发生变更，直接将变更记录保存到pkRecordMap对应的主键中即可。
                     */

                    Long oldPK = Long.parseLong(columnValueBefore);
                    Long newPK = Long.parseLong(columnValueAfter);
                    if(!this.pkRecordMap.containsKey(oldPK)) {
                        int[] recordMetaArray = new int[this.metaArrayInitSize];
                        // 数组的第一个元素存的是数组下一个空闲的位置（也就是当前元素个数）。
                        // 因此要注意数组有效内容是从1开始的
                        // 用int来记录metaData（文件编号+偏移）
                        int metaData = MyUtil.setOffsetMetaData(0, recordOffset);
                        metaData = MyUtil.setFileNOMetaData(metaData, this.curFileNo);
                        recordMetaArray[1] = metaData;
                        recordMetaArray[0] = 2;
                        // 情况 1
                        if(oldPK.longValue() != newPK.longValue()) {
                            ++this.pkUpdateCnt;
                            // 情况 1.(1)
                            PkUpdateRecord pur = new PkUpdateRecord();
                            pur.modifyType = Constants.UPDATE_TYPE;
                            //pur.pkBeforeModify = oldPK;
                            pur.pkAfterModify = newPK;
                            this.pkUpdateNotInRange.put(oldPK, pur);
                            this.pkRecordMap.put(newPK, recordMetaArray);
                            // 在pkRecordReverseMap中保存对应的pkRecordMap->pkUpdateNotInRange的映射；
                            this.pkRecordReverseMap.put(newPK, oldPK);
                        }else{
                            // 情况 1.(2):如果主键没有发生变更,只在pkRecordMap中插入对应主键的变更记录即可；
                            this.pkRecordMap.put(newPK, recordMetaArray);
                        }
                        recordMetaArray = null;
                    }else{
                        // 用int来记录metaData（文件编号+偏移）
                        int metaData = MyUtil.setOffsetMetaData(0, recordOffset);
                        metaData = MyUtil.setFileNOMetaData(metaData, this.curFileNo);
                        // 情况2
                        if(oldPK.longValue() != newPK.longValue()) {
                            ++this.pkUpdateCnt;
                            // 情况2.(1):
                            // 不能直接在pkUpdateNotInRange中查询，因为pk可能变更了好多次
                            // 而pkUpdateNotInRange保存的是第一次变更时的旧PK
                            // 需要通过reverseMap间接查询
                            boolean hasKey = true;
                            // 通过pkRecordReverseMap能够查询到当前oldPk对应的第一次变更的pk的值
                            final Long firstPK = this.pkRecordReverseMap.get(oldPK);
                            if(firstPK == null) {
                                // 说明pkUpdateNotInRange中没有该主键的记录
                                hasKey = false;
                            }
                            if(hasKey) {
                                // 进一步查询
                                hasKey = this.pkUpdateNotInRange.containsKey(firstPK);
                            }
                            if(!hasKey) {
                                // 情况2.(1).(a)
                                //在pkUpdateNotInRange中插入相关记录
                                PkUpdateRecord pur = new PkUpdateRecord();
                                pur.modifyType = Constants.UPDATE_TYPE;
                                //pur.pkBeforeModify = oldPK;
                                pur.pkAfterModify = newPK;
                                this.pkUpdateNotInRange.put(oldPK,pur);
                                // 删除pkRecordMap中对应的老键，保存新键并保存变更记录的meta
                                int[] metaArray = this.pkRecordMap.remove(oldPK);
                                addMetaData2RecordMap(metaArray, metaData, newPK, true);
                                // 在pkRecordReverseMap中保存
                                // 对应的pkRecordMap->pkUpdateNotInRange的映射；
                                this.pkRecordReverseMap.put(newPK, oldPK);
                            }else{
                                // 情况2.(1).(b)
                                // 修改pkUpdateNotInRange的pkAfterModify的值，然后修改pkRecordMap并保存变更记录，并更新
                                // pkRecordReverseMap映射；
                                this.pkUpdateNotInRange.get(firstPK).pkAfterModify = newPK;
                                // 去掉旧的映射
                                this.pkRecordReverseMap.remove(oldPK);
                                // 保存新的映射
                                this.pkRecordReverseMap.put(newPK, firstPK);
                                // 删除pkRecordMap中对应的键，保存新键并增加变更记录的meta
                                int[] metaArray = this.pkRecordMap.remove(oldPK);
                                addMetaData2RecordMap(metaArray, metaData, newPK, true);
                            }
                        }else{
                            // 情况2.(2):主键没有发生变更，直接将变更记录保存到pkRecordMap对应的主键中即可。
                            int[] metaArray = this.pkRecordMap.get(oldPK);
                            addMetaData2RecordMap(metaArray, metaData, oldPK, false);
                        }
                    }

                }
                // 然后直接移到下一行变更记录
                while (dataBuf.hasRemaining()) {
                    if (dataBuf.get() == '\n') {
                        break;
                    }
                }
                break;
            } // end if(pk == '1')
            else{
                // 否则继续读到主键为止，注意skip掉dataBuf当前指向的那个'|'
                dataBuf.get();
            }
        }
    }

    /**
     *
     * @param metaArray
     * @param metaData
     * @param pk
     * @param putAnyway 如果是true,那么不管是否动态增长了都要再Put进pkRecordMap
     */
    private void addMetaData2RecordMap(int[] metaArray, final int metaData,
                                       final Long pk, final boolean putAnyway) {
        int nxtIdx = metaArray[0];
        if(nxtIdx >= metaArray.length) {
            // 满了,动态增长
            int[] newMetaArray = growAndCopyArray(metaArray);
            newMetaArray[nxtIdx++] = metaData;
            newMetaArray[0] = nxtIdx;
            // 然后替换原来的数组
            metaArray = null;
            this.pkRecordMap.put(pk, newMetaArray);
            newMetaArray = null;
        }else{
            // 大小还够
            metaArray[nxtIdx++] = metaData;
            metaArray[0] = nxtIdx;
            if(putAnyway) {
                this.pkRecordMap.put(pk, metaArray);
            }
        }
    }
}
