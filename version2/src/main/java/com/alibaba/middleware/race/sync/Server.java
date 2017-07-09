package com.alibaba.middleware.race.sync;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 服务器类
 * Created by Jenson on 2017/6/7.
 */
public class Server {
    static Logger logger = LoggerFactory.getLogger(Server.class);

    private SocketWriteCache socketWriteCache;
    /* TODO:用于探索数据特征 */
    private int insertCnt = 0;
    private int updateCnt = 0;
    private int deleteCnt = 0;
    private int pkUpdateCnt = 0;
    private int pkNum = 0;
    private int pkFinalValMapSize = 0;
    private int pkHashValSize = 0;
    private int pkOutRangeChange2InRange = 0;
    // 用来统计结果主键的列数目范围
    private int minColumnNum = 10000;
    private int maxColumnNum = -1;
    // 统计有多少个不同的列名
    private HashSet<String> columnNameSet = new HashSet<>();
    // 如果某个列是字符串，那么不同列值的个数
    private HashSet<String> strColumnValueSet = new HashSet<>();
    // 字符串列值的最大长度
    private int strColumnMaxLen = 0;
    private int strColumnMinLen = 1000;
    // 数字类型列值的最大值
    private long longColumnMaxVal = 0;
    private long longColumnMinVal = 100000000;
    // 统计下某个列出现在第几列的范围
    private int firstNameMinColumnNO = 100000;
    private int firstNameMaxColumnNO = -1;
    private int lastNameMinColumnNO = 100000;
    private int lastNameMaxColumnNO = -1;
    private int sexMinColumnNO = 100000;
    private int sexMaxColumnNO = -1;
    private int scoreMinColumnNO = 100000;
    private int scoreMaxColumnNO = -1;
    private int score2MinColumnNO = 100000;
    private int score2MaxColumnNO = -1;

    //private HashSet<String> pkSet = new HashSet<>();
    private ArrayList<Long> fileSizeList = new ArrayList<>(10);

    // 统计每个键大概有多少条操作记录
    private int minOPNumPerRecord = 100000000;
    private int maxOPNumPerRecord = -1;
    private int totalOPNum = 0;
    // 记录主键的大小范围
    private Integer minPK = Integer.MAX_VALUE;
    private Integer maxPK = -1;
    // 统计时间
    private long constructPKChangedRecordTime = 0;
    private long workerTime = 0;
    private long replayTime = 0;

    private final String schema;
    private final String tableName;
    private final Integer startPkId;
    private final Integer endPkId;


    private final int FILE_NO_LIMIT = Constants.THREAD_NUM;

    // 最终结果
    private ArrayList<Record> finalRecordList;

    // 从原文件读取字节到lineBytes，一次处理一行
    private final byte[] lineBytes = new byte[512];
    // 用于保存主键的一系列变更记录，例如PK变更为：2->3->4->23，
    // 那么key=23, value=[2,3,4]
    private HashMap<Integer, int[]> pkChangedRecord;
    private final int pkChangedArrayInitSize = 2;
    // 根据pkChangedRecord来保存某个最终在范围内的pk的最后的值
    private HashMap<Integer, Integer> pkFinalValMap;
    /* 6/17 更新：将原10个文件的变更记录根据PK 哈希到13个文件中 */
    // 如果某个PK被修改过，以该PK第一次插入时hash的值为准
    private HashMap<Integer, Byte> pkHashValue;
    private ArrayList<FileWriteCache> midFileWriteCache;
    private Worker[] workers;

    // 统计每个键变更次数的范围
    private int minPkChangedCnt = 10000000;
    private int maxPkChangedCnt = -1;
    private int totalPKChangedCnt = 0;

    public Server(final String[] args) {
        /**
         * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
         * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
         * id>100 and id<200
         */
        // 第一个参数是Schema Name
        this.schema = args[0];
        // 第二个参数是table Name
        this.tableName = args[1];
        // 第三个参数是start pk Id
        this.startPkId = Integer.parseInt(args[2]);
        // 第四个参数是end pk Id
        this.endPkId = Integer.parseInt(args[3]);


        this.finalRecordList = new ArrayList<>(1024);
        this.pkChangedRecord = new HashMap<>(1024);
        this.pkFinalValMap = new HashMap<>(1024);
        this.pkHashValue = new HashMap<>(1024);

        this.midFileWriteCache = new ArrayList<>(Constants.MIDDLE_FILE_NUM);
        for(int i = 0; i < Constants.MIDDLE_FILE_NUM; ++i) {
            this.midFileWriteCache.add(new FileWriteCache(i));
        }
    }

    public void startServer(int port) throws InterruptedException {
        this.socketWriteCache = new SocketWriteCache(port);

        long startTime = System.currentTimeMillis();
        constructPKChangedRecord();
        long endTime = System.currentTimeMillis();
        this.constructPKChangedRecordTime = (endTime-startTime);
        logger.info("constructPKChangedRecord takes:"+constructPKChangedRecordTime+" ms");

        startTime = System.currentTimeMillis();
        startWorkers();
        endTime = System.currentTimeMillis();
        this.workerTime = endTime - startTime;
        logger.info("workerTime :"+workerTime+" ms");

        startTime = System.currentTimeMillis();
        sendLogMsg2Client();
        endTime = System.currentTimeMillis();

        logger.info("Send result to client takes:"+(endTime-startTime)+" ms");
        //logger.info(result);
        //System.out.println("Server finish running");
    }

    private void constructPKChangedRecord() {
        int curFileNo = 1;
        while(curFileNo <= this.FILE_NO_LIMIT) {
            // 开始解析文件
            final String fileName = Paths.get(Constants.DATA_HOME, curFileNo + ".txt").toString();
            File file = new File(fileName);
            if (!file.exists()) {
                logger.info(fileName + " doesnot exists");
                break;
            }
            // 原数据文件
            RandomAccessFile dataRaf = null;
            MappedByteBuffer mapFile = null;
            try {
                // 原数据文件
                dataRaf = new RandomAccessFile(file, "r");
                mapFile = dataRaf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, dataRaf.length());
            } catch (FileNotFoundException e) {
                logger.info(e.getMessage(), e);
            } catch (IOException e) {
                logger.info(e.getMessage(), e);
            }
            // 用于计算'|'的数目
            int verticalLineCnt = 0;
            int prePos = 0, newPos = 0, len = 0;
            while(mapFile.hasRemaining()) {
                verticalLineCnt = 0;
                // 开始解析
                // 这就是当前变更记录的起始位置
                prePos = mapFile.position();
                // 读了3个'|'后就是schema的第一个字符
                while(mapFile.hasRemaining()) {
                    byte b = mapFile.get();
                    if(b == '|') {
                        if(++verticalLineCnt >= 3) {
                            break;
                        }
                    }
                }
                mapFile.position(mapFile.position()+Constants.SKIP_POS);

                // 下一个字符就是'I U D'了
                final byte modifiedType = mapFile.get();
                // 读掉后面的'|'
                mapFile.get();
                if(modifiedType == 'I') {
                    ++this.insertCnt;
                    extractPKChangedInfo(mapFile, Constants.INSERT_TYPE);
                }else if(modifiedType == 'U') {
                    ++this.updateCnt;
                    extractPKChangedInfo(mapFile, Constants.UPDATE_TYPE);
                }else if(modifiedType == 'D') {
                    ++this.deleteCnt;
                    extractPKChangedInfo(mapFile, Constants.DELETE_TYPE);
                }else{
                    logger.info("Error modify type:"+modifiedType);
                    System.out.println("Error modify type:"+modifiedType);
                }
            }
            if(dataRaf != null) {
                try {
                    dataRaf.close();
                } catch (IOException e) {
                    logger.info(e.getMessage(), e);
                }
            }
            ++curFileNo;
        }

        this.pkHashValSize = this.pkHashValue.size();
        // 不需要了
        this.pkHashValue.clear();
        this.pkHashValue = null;

        for(final FileWriteCache fwc : this.midFileWriteCache) {
            fwc.flush();
            fwc.close();
        }
        this.midFileWriteCache.clear();
        this.midFileWriteCache = null;

        logger.info("PK num before remove:"+this.pkChangedRecord.size());
        System.out.println("PK num before remove:"+this.pkChangedRecord.size());
        for(Iterator<Map.Entry<Integer, int[]>> it = this.pkChangedRecord.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, int[]> entry = it.next();
            Integer pk = entry.getKey();
            //int[] array = entry.getValue();
            if(pk.intValue() <= this.startPkId.intValue() || pk.intValue() >= this.endPkId.intValue()) {
                it.remove();
            }
        }
        this.pkNum = this.pkChangedRecord.size();

        // 构造pkFinalValMap
        for(Iterator<Map.Entry<Integer, int[]>> it = this.pkChangedRecord.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, int[]> entry = it.next();
            Integer finalPK = entry.getKey();
            int[] oldPkVals = entry.getValue();
            final int num = oldPkVals[0]-1;
            if(num > 0) {
                // TODO:统计主键变更次数的最大最小值
                if(num > this.maxPkChangedCnt) {
                    this.maxPkChangedCnt = num;
                }
                if(num < this.minPkChangedCnt) {
                    this.minPkChangedCnt = num;
                }
                this.totalPKChangedCnt += num;
                for(int i = 1; i <= num; ++i) {
                    // 曾经的值
                    Integer oldPK = oldPkVals[i];
                    // TODO 统计有多少主键曾经不在范围，但是变更到了范围内
                    if(oldPK.intValue() <= this.startPkId.intValue()
                            || oldPK.intValue() >= this.endPkId.intValue()) {
                        ++this.pkOutRangeChange2InRange;
                    }
                    this.pkFinalValMap.put(oldPK, finalPK);
                }
            }
            // 自己也要存！
            this.pkFinalValMap.put(finalPK, finalPK);
            it.remove();
        }
        this.pkChangedRecord.clear();
        this.pkChangedRecord = null;
    }

    private void startWorkers() {
        this.workers = new Worker[Constants.MIDDLE_FILE_NUM];
        final CountDownLatch latch = new CountDownLatch(Constants.MIDDLE_FILE_NUM);
        ArrayList<HashMap<Integer, Record>> workerResults = new ArrayList<>(Constants.MIDDLE_FILE_NUM);
        for(int i = 0; i < Constants.MIDDLE_FILE_NUM; ++i) {
            workerResults.add(new HashMap<Integer, Record>());
            Worker worker = new Worker(i, workerResults.get(i), this.pkFinalValMap);
            worker.setCountDownLatch(latch);
            this.workers[i] = worker;
            new Thread(worker).start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
        }
        // 可以清理掉pkFinalVal了
        for(int i = 0; i < Constants.MIDDLE_FILE_NUM; ++i) {
            this.workers[i].releaseResource();
            this.workers[i] = null;
        }
        this.workers = null;
        this.pkFinalValMapSize = pkFinalValMap.size();
        this.pkFinalValMap.clear();
        this.pkFinalValMap = null;
        for(int i = 0; i < Constants.MIDDLE_FILE_NUM; ++i) {
            HashMap<Integer, Record> result = workerResults.get(i);
            if(!result.isEmpty()) {
                this.finalRecordList.addAll(result.values());
            }
        }
        workerResults.clear();
        workerResults = null;

        Collections.sort(this.finalRecordList);
    }


    private void extractPKChangedInfo(final ByteBuffer dataBuf, final byte modifiedType) {
        // 格式：id:1:1|6577137|6577137|......
        // 提取出PK的变更信息
        while(dataBuf.hasRemaining()) {
            // skip到主键部分内容
            while(dataBuf.hasRemaining()) {
                if(dataBuf.get() == '|') {
                    break;
                }
            }
            int pkBefore = 0;
            int pkAfter = 0;
            if(modifiedType == Constants.INSERT_TYPE) {
                // 只有变更后的值
                // skip变更前的值
                while(dataBuf.get() != '|') {
                }
                // 计算变更后的值
                byte b;
                while((b = dataBuf.get()) != '|') {
                    pkAfter = pkAfter*10 + (b-'0');
                }
                // 如果不在范围，skip掉
                if(pkAfter <= this.startPkId.intValue()
                        || pkAfter >= this.endPkId.intValue()) {
                    while(dataBuf.get() != '\n') {
                    }
                    return;
                }
                int[] pkChangedRecordArray = new int[this.pkChangedArrayInitSize];
                pkChangedRecordArray[0] = 1;
                this.pkChangedRecord.put(pkAfter, pkChangedRecordArray);
                // 将剩下的内容写到中间文件
                int idx = 0;
                while((b = dataBuf.get()) != '\n') {
                    this.lineBytes[idx++] = b;
                }
                // 虽然是插入，但其实之前也可能插入过一样的PK
                Byte oldHashVal = this.pkHashValue.get(pkAfter);
                int hashVal;
                if(oldHashVal == null) {
                    hashVal = pkAfter % Constants.MIDDLE_FILE_NUM;
                }else{
                    hashVal = oldHashVal.intValue();
                }
                FileWriteCache fwc = this.midFileWriteCache.get(hashVal);
                fwc.write(modifiedType, pkBefore, pkAfter, this.lineBytes, 0, idx);

            }else if(modifiedType == Constants.DELETE_TYPE) {
                // 只有变更前的值
                byte b;
                while((b = dataBuf.get()) != '|') {
                    pkBefore = pkBefore*10 + (b-'0');
                }
                // skip掉变更后的NULL
                while(dataBuf.get() != '|') {
                }
                // 删除变更只有变更前列值
                this.pkChangedRecord.remove(pkBefore);

                // 删除变更只需要存Delete,pk 即可
                while(dataBuf.get() != '\n') {

                }
                // 如果不在范围，skip掉
                if(pkBefore <= this.startPkId.intValue()
                        || pkBefore >= this.endPkId.intValue()) {
                    return;
                }
//                // 将剩下的内容写到中间文件
//                int idx = 0;
//                while((b = dataBuf.get()) != '\n') {
//                    this.lineBytes[idx++] = b;
//                }
                // 虽然是删除，但其实之前也可能插入过一样的PK
                Byte oldHashVal = this.pkHashValue.get(pkBefore);
                int hashVal;
                if(oldHashVal == null) {
                    hashVal = pkBefore % Constants.MIDDLE_FILE_NUM;
                }else{
                    hashVal = oldHashVal.intValue();
                }
                FileWriteCache fwc = this.midFileWriteCache.get(hashVal);
                fwc.write(modifiedType, pkBefore, pkAfter, this.lineBytes, 0, 0);

            }else if(modifiedType == Constants.UPDATE_TYPE) {
                byte b;
                while((b = dataBuf.get()) != '|') {
                    pkBefore = pkBefore*10 + (b-'0');
                }
                while((b = dataBuf.get()) != '|') {
                    pkAfter = pkAfter*10 + (b-'0');
                }
                // 如果不在范围，skip掉
                if(pkAfter <= this.startPkId.intValue()
                        || pkAfter >= this.endPkId.intValue()) {
                    while(dataBuf.get() != '\n') {
                    }
                    // 并且把原来的也删除掉
                    this.pkChangedRecord.remove(pkBefore);
                    return;
                }

                int hashVal;
                /* 需要记录所有的变更 */
                // 变更后的pk的hash值和变更前一样
                // 如果变更前的PK也发生过变更
                Byte pkBeforeHashVal = this.pkHashValue.get(pkBefore);
                if(pkBeforeHashVal == null) {
                    hashVal = pkBefore % Constants.MIDDLE_FILE_NUM;
                }else{
                    hashVal = pkBeforeHashVal.intValue();
                }
                if(pkBefore != pkAfter) {
                    ++this.pkUpdateCnt;
                    int[] oldPkChangedRecordArray = this.pkChangedRecord.remove(pkBefore);
                    if(oldPkChangedRecordArray == null) {
                        logger.info("Error! pkChangedRecord doesnot have key:"+pkBefore );
                        return;
                    }
                    int nxtIdx = oldPkChangedRecordArray[0];
                    if(nxtIdx >= oldPkChangedRecordArray.length) {
                        oldPkChangedRecordArray = growAndCopyArray(oldPkChangedRecordArray, this.pkChangedArrayInitSize);
                    }
                    oldPkChangedRecordArray[nxtIdx++] = pkBefore;
                    oldPkChangedRecordArray[0] = nxtIdx;
                    this.pkChangedRecord.put(pkAfter, oldPkChangedRecordArray);
                    // 如果是发生了主键变更，需要把pkAfter的哈希值记录下来
                    this.pkHashValue.put(pkAfter, (byte)hashVal);
                }
                // 将剩下的内容写到中间文件
                int idx = 0;
                while((b = dataBuf.get()) != '\n') {
                    this.lineBytes[idx++] = b;
                }
                FileWriteCache fwc = this.midFileWriteCache.get(hashVal);
                fwc.write(modifiedType, pkBefore, pkAfter, this.lineBytes, 0, idx);
            }
            return;
        }
    }



    /**
     * 增长并复制数组
     * @param oldArray
     * @return 新的数组
     */
    private int[] growAndCopyArray(final int[] oldArray, final int increase) {
        final int nxtIdx = oldArray[0];
        // 满了,动态增长
        int[] newMetaArray = new int[oldArray.length + increase];
        // 复制过去
        for(int i = 0; i < nxtIdx; ++i) {
            newMetaArray[i] = oldArray[i];
        }
        return newMetaArray;
    }


    // 简单的将结果字符串写到socket里面去
    private void sendLogMsg2Client() {
        logger.info("finalRecordList size="+finalRecordList.size());
        System.out.println("finalRecordList size="+finalRecordList.size());
        for(final Record record : this.finalRecordList) {
            StringBuilder sb = new StringBuilder(64);
            final int columnSize = record.getColumnNameSet().size();
            // TODO:统计列数目的范围
            if(columnSize > this.maxColumnNum) {
                this.maxColumnNum = columnSize;
            }
            if(columnSize < this.minColumnNum) {
                this.minColumnNum = columnSize;
            }
            ArrayList<Column> sortColumnList = new ArrayList<>(columnSize);
            for(final String columnName : record.getColumnNameSet()) {
                Column column = record.getColumn(columnName);
                // TODO:统计各个列顺序
                if(columnName.charAt(0) == 'f') {
                    // first_name
                    if(column.columnNO < this.firstNameMinColumnNO) {
                        this.firstNameMinColumnNO = column.columnNO;
                    }
                    if(column.columnNO > this.firstNameMaxColumnNO) {
                        this.firstNameMaxColumnNO = column.columnNO;
                    }
                }else
                if(columnName.charAt(0) == 'l') {
                    // last_name
                    if(column.columnNO < this.lastNameMinColumnNO) {
                        this.lastNameMinColumnNO = column.columnNO;
                    }
                    if(column.columnNO > this.lastNameMaxColumnNO) {
                        this.lastNameMaxColumnNO = column.columnNO;
                    }
                }else
                if(columnName.charAt(0) == 's' && columnName.charAt(1) == 'e') {
                    // sex
                    if(column.columnNO < this.sexMinColumnNO) {
                        this.sexMinColumnNO = column.columnNO;
                    }
                    if(column.columnNO > this.sexMaxColumnNO) {
                        this.sexMaxColumnNO = column.columnNO;
                    }
                }else if(columnName.charAt(0) == 's' && columnName.charAt(columnName.length()-1) == 'e') {
                    // score
                    if(column.columnNO < this.scoreMinColumnNO) {
                        this.scoreMinColumnNO = column.columnNO;
                    }
                    if(column.columnNO > this.scoreMaxColumnNO) {
                        this.scoreMaxColumnNO = column.columnNO;
                    }
                }else if(columnName.charAt(0) == 's' && columnName.charAt(columnName.length()-1) == '2') {
                    // score
                    if(column.columnNO < this.score2MinColumnNO) {
                        this.score2MinColumnNO = column.columnNO;
                    }
                    if(column.columnNO > this.score2MaxColumnNO) {
                        this.score2MaxColumnNO = column.columnNO;
                    }
                }

                // TODO:统计不同列名的个数
                this.columnNameSet.add(columnName);
                sortColumnList.add(column);
            }
            Collections.sort(sortColumnList);
            // 先放主键
            sb.append(record.getPK());
            for(final Column column : sortColumnList) {


                sb.append('\t');
                if(column.isStr) {
                    sb.append(column.strColumnVal);
                    // TODO:统计字符串类型列值长度
                    if(column.strColumnVal.length() > this.strColumnMaxLen) {
                        this.strColumnMaxLen = column.strColumnVal.length();
                    }
                    if(column.strColumnVal.length() < this.strColumnMinLen) {
                        this.strColumnMinLen = column.strColumnVal.length();
                    }
                    if(this.strColumnValueSet.size() < 300000) {
                        this.strColumnValueSet.add(column.strColumnVal);
                    }
                }else{
                    sb.append(column.longColumnVal);
                    // TODO:统计数字类型列值的最大值
                    if(column.longColumnVal > this.longColumnMaxVal) {
                        this.longColumnMaxVal = column.longColumnVal;
                    }
                    if(column.longColumnVal < this.longColumnMinVal) {
                        this.longColumnMinVal = column.longColumnVal;
                    }
                }
            }
            sb.append('\n');
            //System.out.println(sb.toString());
            byte[] resultBytes = sb.toString().getBytes();
            this.socketWriteCache.writeData(resultBytes, resultBytes.length);
        }
        this.finalRecordList.clear();
        this.finalRecordList = null;

        this.socketWriteCache.flush();
        this.socketWriteCache.close();

        StringBuilder sb = new StringBuilder();
        sb.append("Pk number:").append(this.pkNum)
                .append(" pkUpdateCnt:").append(pkUpdateCnt)
                .append(" inserCnt:").append(insertCnt)
                .append(" updateCnt:").append(updateCnt)
                .append(" deleteCnt:").append(deleteCnt).append('\n');
        sb.append("Min PK:").append(minPK).append(" Max PK:").append(maxPK).append('\n');
        sb.append("minOPNumPerRecord=").append(minOPNumPerRecord).
                append(" maxOPNumPerRecord=").append(maxOPNumPerRecord)
                .append(" totalOPNum=").append(totalOPNum).append("\n");
        sb.append("startPkId=").append(startPkId).append(" endPkId=").append(endPkId).append('\n');
        sb.append("minPkChangedCnt=").append(this.minPkChangedCnt)
                .append(" maxPkChangedCnt=").append(this.maxPkChangedCnt)
                .append(" 范围内主键变更次数:").append(this.totalPKChangedCnt)
                .append('\n');
        sb.append("范围外主键变更到范围内次数:").append(this.pkOutRangeChange2InRange).append('\n');
        sb.append("Pk final value map size=").append(this.pkFinalValMapSize).append('\n');
        sb.append("Pk hash value size=").append(this.pkHashValSize).append('\n');
        sb.append("列数目范围：").append(this.minColumnNum).append(",").append(this.maxColumnNum)
                .append(" 数字类型列值大小范围:").append(this.longColumnMinVal).append(",").append(this.longColumnMaxVal)
                .append(" 字符类型列值长度范围:").append(this.strColumnMinLen).append(",").append(this.strColumnMaxLen)
                .append(" 不同字符类型列值数目：").append(this.strColumnValueSet.size())
                .append(" 总共不同列名:\n");
        for(final String columnName : this.columnNameSet) {
            sb.append(columnName).append(" ");
        }
        sb.append('\n');
        sb.append("first_name列号范围:").append(this.firstNameMinColumnNO).append(",").append(this.firstNameMaxColumnNO)
                .append("\nlast_name列号范围:").append(this.lastNameMinColumnNO).append(",").append(this.lastNameMaxColumnNO)
                .append("\nsex列号范围:").append(this.sexMinColumnNO).append(",").append(this.sexMaxColumnNO)
                .append("\nscore列号范围:").append(this.scoreMinColumnNO).append(",").append(this.scoreMaxColumnNO)
                .append("\nscore2列号范围:").append(this.score2MinColumnNO).append(",").append(this.score2MaxColumnNO).append('\n');
        sb.append("Construct time=").append(this.constructPKChangedRecordTime)
                .append(" Worker time=").append(this.workerTime).append(" replayTime=").append(this.replayTime).append("\n");
//        sb.append("File size:\n");
//        for(final long fsize : this.fileSizeList) {
//            long fsizeMB = (fsize/1024/1024);
//            sb.append(fsizeMB).append(" MB\n");
//        }
        logger.info(sb.toString());
        System.out.println(sb.toString());
    }


    /******** 与Server类无关的static 方法 **/
    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }
    public static void main(String[] args) throws InterruptedException {
        initProperties();
        Server server = new Server(args);
        logger.info("com.alibaba.middleware.race.sync.Server is running....");
        server.startServer(Constants.SERVER_PORT);

    }
}