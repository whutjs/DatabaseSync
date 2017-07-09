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

import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 服务器类
 * Created by Jenson on 2017/6/7.
 */
public class Server {
    static Logger logger = LoggerFactory.getLogger(Worker.class);
    /** 用于统计输入数据信息 */
    // 统计每个键大概有多少条操作记录
    private int totalPKNUM = 0;

    private final String schema;
    private final String tableName;
    private final Long startPkId;
    private final Long endPkId;
    private final int FILE_NO_LIMIT = 10;
    // 用于在根据key的记录reply数据操作时根据文件编号获取对应的文件mapfile
    private ArrayList<MappedByteBuffer> mapFileList;
    private ArrayList<RandomAccessFile> rafList;
    private ArrayList<Long> fileSizeList;

    // 最终结果
    private ArrayList<Record> finalRecordList;

   /** 6/10更新：每条worker线程一个变更记录 */
    private ArrayList<HashMap<Long, int[]>> threadPkRecordList;
    private ArrayList<HashMap<Long, PkUpdateRecord>> threadPkUpdateList;
    private ArrayList<Worker> workerList;
    private final CountDownLatch latch;
    // 最终汇总的结果
    private HashMap<Long, int[]> totalPkRecordMap;
    // 当meta数组长度不够时，每次增长的步长
    private final int INCR_STEP = 9;
    // 用来存需要提取出字符串的bytes
    private final byte[] strBytes = new byte[128];

    /** Socket */
    private Socket client;

    public Server(final String[] args) {
        /**
         * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
         * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
         * id>100 and id<200
         */
        // 第一个参数是Schema Name
        this.schema = args[0];
        logger.info("Schema:"+this.schema);
        // 第二个参数是table Name
        this.tableName = args[1];
        logger.info("tableName:"+this.tableName);
        // 第三个参数是start pk Id
        this.startPkId = Long.parseLong(args[2]);
        logger.info("startPkId:"+this.startPkId);
        // 第四个参数是end pk Id
        this.endPkId = Long.parseLong(args[3]);
        logger.info("startPkId:"+this.endPkId);

        this.latch = new CountDownLatch(Constants.THREAD_NUM);
        this.threadPkRecordList = new ArrayList<>(Constants.THREAD_NUM);
        this.threadPkUpdateList = new ArrayList<>(Constants.THREAD_NUM);
        this.workerList = new ArrayList<>(Constants.THREAD_NUM);
        for(int i = 0; i < Constants.THREAD_NUM; ++i) {
            this.threadPkRecordList.add(new HashMap<Long, int[]>(100));
            this.threadPkUpdateList.add(new HashMap<Long, PkUpdateRecord>());
        }
        this.mapFileList = new ArrayList<>(10);
        this.rafList = new ArrayList<>(10);
        this.fileSizeList = new ArrayList<>(10);
        this.finalRecordList = new ArrayList<>();

    }

    public void startServer(int port) throws InterruptedException {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            client = serverSocket.accept();
        } catch (Exception e) {
            logger.info("服务器异常: " + e.getMessage(), e);
        }
        logger.info("IsConnected:"+this.client.isConnected());
        logger.info("Start server");
        openFile();
        splitFiles2Workers();
        startWorkers();
        mergeWorkerResult();
        removeKeyNotInRange();
        replayRecord();
        closeFiles();
        String result = logRecord();
        //logger.info(result);
        sendLogMsg2Client(result);

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
//            logger.info("In getStrEndWithVerticalLine, mapFile curPos:"+newPos);
            return "";
        }
        // NOTE:注意这里要让dataBuf的位置恢复到字符串第一个字符的位置！！
        dataBuf.position(prePos);
        if(len > strBytes.length ) {
            byte[] tmpBytes = new byte[len];
            dataBuf.get(tmpBytes);
            String longStr = new String(tmpBytes);
            logger.info("Found long str:len="+len + " str:"+longStr);
            return longStr;
        }else {
            // NOTE:读完之后，dataBuf再次指向的是schema后面的'|'!!!!
            dataBuf.get(strBytes, 0, len);
            return new String(strBytes, 0, len);
        }
    }



    /**
     * 打开10个文件并保存对应文件的mapfile
     */
    private void openFile() {
        int newFileNo = 1;
        while(newFileNo <= this.FILE_NO_LIMIT) {
            // 开始解析文件
            final String fileName = Paths.get(Constants.DATA_HOME, newFileNo + ".txt").toString();
            File file = new File(fileName);
            if (!file.exists()) {
                logger.info(fileName + " doesnot exists");
                break;
            }
            try {
                RandomAccessFile newRaf = new RandomAccessFile(file, "r");
                final long fileLen = newRaf.length();
                 MappedByteBuffer newMapFile = newRaf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileLen);
                this.rafList.add(newRaf);
                this.mapFileList.add(newMapFile);
                this.fileSizeList.add(fileLen);
            } catch (FileNotFoundException e) {
                logger.info("Error in Server.336. "+e.getMessage(), e);
                e.printStackTrace();
            } catch (IOException e) {
                logger.info("Error in Server.339. "+e.getMessage(), e);
                e.printStackTrace();
            }
            ++newFileNo;
        }
    }

    /**
     * 将10个文件切割成20份分给20条线程来读
     */
    private void splitFiles2Workers() {
        final int fileNum = this.mapFileList.size();
        int threadNO = 0;
        for(int i = 0; i < fileNum; ++i) {
            final int curFileNo = i+1;
            final long fileSize = this.fileSizeList.get(i);
            System.out.println(curFileNo+".txt size:" +fileSize);
            // 切割成两部分
            final int halfSize = (int)(fileSize / 2);
            MappedByteBuffer mapFile = this.mapFileList.get(i);
            // 找到文件位于中间部分的'\n'
            mapFile.clear();
            mapFile.position(halfSize);
            while(mapFile.hasRemaining()) {
                if(mapFile.get() == '\n') {
                    break;
                }
            }
            int splitPos = mapFile.position();
            // 那么前半部分范围就是：[0, splitPos)
            mapFile.position(0);
            mapFile.limit(splitPos);
            ByteBuffer topHalfMapFile = mapFile.slice();
            Worker topHalfWorker = new Worker(threadNO, curFileNo, 0, this.schema, this.tableName,
                    this.threadPkRecordList.get(threadNO), this.threadPkUpdateList.get(threadNO),
                    topHalfMapFile,latch);
            this.workerList.add(topHalfWorker);
            // 后半部分的范围就是: [splitPos, fileSize);
            ++threadNO;
            mapFile.clear();
            mapFile.position(splitPos);
            ByteBuffer bottomHalfMapFile = mapFile.slice();
            Worker bottomHalfWorker = new Worker(threadNO, curFileNo,splitPos, this.schema, this.tableName,
                    this.threadPkRecordList.get(threadNO), this.threadPkUpdateList.get(threadNO),
                    bottomHalfMapFile,latch);
            this.workerList.add(bottomHalfWorker);
            ++threadNO;
        }
    }

    /**
     * 启动worker线程并等待结束
     */
    private void startWorkers() {
        logger.info("startWorkers");
        for(final Worker worker : this.workerList) {
            new Thread(worker).start();
        }
        try {
            this.latch.await();
        } catch (InterruptedException e) {
            logger.error("Error in Server.363,", e);
        }
    }

    /**
     * 合并worker的结果
     */
    private void mergeWorkerResult() {
        // 保存汇总的结果
        this.totalPkRecordMap = new HashMap<>(1000);
        final int workerNum = this.workerList.size();
        int curWorker = 0;
        while(curWorker < workerNum) {
            HashMap<Long, int[]>  pkRecordToBeMerged = this.threadPkRecordList.get(curWorker);
            HashMap<Long, PkUpdateRecord> pkUpdateRecordToBeMerged = this.threadPkUpdateList.get(curWorker);
            mergeWorker2TotalResult(pkRecordToBeMerged, pkUpdateRecordToBeMerged, curWorker);
            // 可以清空了
            pkRecordToBeMerged.clear();;
            pkUpdateRecordToBeMerged.clear();
            ++curWorker;
        }
        // merge完之后，可以清空worker数据了
        for(final Worker worker : this.workerList) {
            worker.releaseResource();
        }
        this.workerList.clear();
        this.threadPkRecordList.clear();
        this.threadPkUpdateList.clear();
        this.totalPKNUM = this.totalPkRecordMap.size();
    }

    /**
     * 将worker的结果merge到最终结果
     * @param pkRecordToBeMerged
     * @param pkUpdateRecordToBeMerged
     */
    private void mergeWorker2TotalResult(HashMap<Long, int[]>  pkRecordToBeMerged,
                               HashMap<Long, PkUpdateRecord> pkUpdateRecordToBeMerged, final int curWorkerNo) {
        // 先执行主键变更
        for(final Long key : pkUpdateRecordToBeMerged.keySet()) {
            PkUpdateRecord pur = pkUpdateRecordToBeMerged.get(key);
            if(pur.modifyType == Constants.DELETE_TYPE) {
                // 在totalPkRecordMap中删除对应的key
               int[] oldVal = this.totalPkRecordMap.remove(key);
               if(oldVal == null) {
//                   logger.error("Error in Server.mergeWorker2TotalResult.395: totalPkRecordMap does not have key:"
//                           +key);
               }
            }else if(pur.modifyType == Constants.UPDATE_TYPE) {
                int[] metadata = this.totalPkRecordMap.remove(key);
                if(metadata == null) {
                    logger.error("Error in Server.mergeWorker2TotalResult.401: totalPkRecordMap does not have key:"
                            +key);
                    continue;
                }
                // 直接replace
                this.totalPkRecordMap.put(pur.pkAfterModify, metadata);
            }
        }
        // 然后merge结果
        for(final Long pk : pkRecordToBeMerged.keySet()) {
            int[] totalMetaData = this.totalPkRecordMap.get(pk);
            int[] metaDataToBeMerged = pkRecordToBeMerged.get(pk);
            if(totalMetaData == null) {
                // 如果是null，说明是在worker范围里面插入的新的记录
                totalMetaData = metaDataToBeMerged;
                // 放入汇总的结果
                this.totalPkRecordMap.put(pk, totalMetaData);
                metaDataToBeMerged = null;
                totalMetaData = null;
                continue;
            }
            // 否则需要把metaDataToBeMerged复制到totalMetaData的后面
            // 需要复制的元素个数
            final int numToBeMerged = metaDataToBeMerged[0]-1;
            int curTotalMetaDataNum = totalMetaData[0]-1;
            // 除了有效元素个数，还有一个空间用来表示数组个数
            final int spaceNeeded = numToBeMerged + curTotalMetaDataNum + 1;
            if(spaceNeeded >= totalMetaData.length) {
                // 需要grow
                totalMetaData = grow2SizeAndCopyArray(spaceNeeded + this.INCR_STEP, totalMetaData);
                this.totalPkRecordMap.put(pk, totalMetaData);
            }
            // 复制到totalMetaData的末尾
            int totalMetaDataIdx = totalMetaData[0];
            int metaDataToBeMergedIdx = 1;
            while(metaDataToBeMergedIdx <= numToBeMerged) {
                totalMetaData[totalMetaDataIdx++] = metaDataToBeMerged[metaDataToBeMergedIdx++];
            }
            totalMetaData[0] = totalMetaDataIdx;
        }
    }

    /**
     * 将数组增长到指定大小并复制元素
     * @param targetSize
     * @param oldArray
     * @return
     */
    private int[] grow2SizeAndCopyArray(final int targetSize, final int[] oldArray) {
        final int nxtIdx = oldArray[0];
        // 满了,动态增长
        int[] newMetaArray = new int[targetSize];
        // 复制过去
        for(int i = 0; i < nxtIdx; ++i) {
            newMetaArray[i] = oldArray[i];
        }
        return newMetaArray;
    }
    private void closeFiles() {
        for(final RandomAccessFile nraf : this.rafList) {
            if(nraf != null) {
                try {
                    nraf.close();
                } catch (IOException e) {
                    logger.info("Error in Server.352. "+e.getMessage(), e);
                    e.printStackTrace();
                }
            }
        }
        this.rafList.clear();
        this.mapFileList.clear();
    }
    /**
     * 删除不在范围内的key
     */
    private void removeKeyNotInRange() {
        for(Iterator<Map.Entry<Long, int[]>> it = this.totalPkRecordMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, int[]> entry = it.next();
            Long pk = entry.getKey();
            if(pk.longValue() <= this.startPkId.longValue() || pk.longValue() >= this.endPkId.longValue()) {
                it.remove();
            }
        }
    }

    /**
     * 从该主键的变更记录中构造最终的记录结果
     * @param pk
     * @param metaArray
     * @return
     */
    private Record getRecordFromMetaArray(final Long pk, final int[] metaArray) {
        Record record = new Record(pk);
        final int arraySize = metaArray.length;
        final int num = metaArray[0];
        for(int i = 1; i < num; ++i) {
            int meta = metaArray[i];
            replayModifyBasedOnMetadata(record, meta);
        }
        return record;
    }

    /**
     * 基于metadata从文件中读数据对record进行操作
     * @param record
     * @param metadata
     */
    private void replayModifyBasedOnMetadata(final Record record, final int metadata) {
        final int fileNO = MyUtil.getFileNO(metadata);
        final int offset = MyUtil.getOffset(metadata);
        MappedByteBuffer dataBuf = this.mapFileList.get(fileNO-1);
        dataBuf.position(offset);
        int startPos = offset;
        while(dataBuf.hasRemaining()) {
            // 找到变更记录起始的'|'
            if(dataBuf.get() == '|') {
                break;
            }
        }
        // startPos指向的就是变更记录有效内容的第一个字符，也就是schema的第一个字符
        startPos = dataBuf.position();
        // skip过两个'|'就是变更类型的字符
        int verticalLineCnt = 0;
        while(dataBuf.hasRemaining()) {
            if(dataBuf.get() == '|') {
                if(++verticalLineCnt >= 2) {
                    break;
                }
            }
        }
        // 现在dataBuf指向变更类型
        final byte modifyType = dataBuf.get();
        // skip '|'
        dataBuf.get();
        int preIdx = 0, idx = 0;
        // 表示列序号
        int columnNO = 0;
         // 循环提取列信息
        while(dataBuf.hasRemaining()) {
            // 要看看第一个字符是不是'\n'，如果是就说明结束了
            preIdx = dataBuf.position();
            if(dataBuf.get() == '\n') {
                break;
            }else{
                // 如果不是，回退一位
                dataBuf.position(preIdx);
            }
            // dataBuf当前位置需要指向列的第一个有效字符，不能是'|'
            String columnInfo = getStrEndWithVerticalLine(dataBuf);
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
            if(modifyType == 'I') {
                Column column = new Column();
                // 插入，需要记录列顺序
                column.columnNO = columnNO;
//                column.columnName = columnName;
                // 插入变更只有变更后列值
                if(valueType == '1') {
                    // 数字类型，也就是Long
                    column.isStr = false;
                    column.longColumnVal = Long.parseLong(columnValueAfter);
                }else{
                    column.isStr = true;
                    column.strColumnVal = columnValueAfter;
                }
                record.addColumn(columnName, column);
            }else if(modifyType == 'U') {
                if(!columnValueBefore.equals(columnValueAfter)) {
                    // 更新,会有更新前列值和更新后列值。
                    Column column = record.getColumn(columnName);
                    if (column == null) {
                        logger.info("Error!!!!Does not have column:" + columnValueBefore);
                        return;
                    }
                    if(valueType == '1') {
                        column.longColumnVal = Long.parseLong(columnValueAfter);
                    }else{
                        column.strColumnVal = columnValueAfter;
                    }
                }
            }
            // skip '|'
            dataBuf.get();
            ++columnNO;
        }

    }


    // 简单的将结果字符串写到socket里面去
    private void sendLogMsg2Client(final String result) {
//        StringBuilder sb = new StringBuilder();
//        sb.append("Pk number:").append(this.pkNum)
//                .append(" pkUpdateCnt:").append(pkUpdateCnt)
//                .append(" inserCnt:").append(insertCnt)
//                .append(" updateCnt:").append(updateCnt)
//                .append(" deleteCnt:").append(deleteCnt).append('\n');
//        sb.append("minOPNumPerRecord=").append(minOPNumPerRecord).
//                append(" maxOPNumPerRecord=").append(maxOPNumPerRecord)
//                .append(" totalOPNum=").append(totalOPNum).append("\n");
//        sb.append("startPkId=").append(startPkId).append(" endPkId=").append(endPkId).append('\n');
//        sb.append("File size:\n");
//        for(final long fsize : this.fileSizeList) {
//            long fsizeMB = (fsize/1024/1024);
//            sb.append(fsizeMB).append(" MB\n");
//        }
//        logger.info(sb.toString());

        final byte[] resultBytes = result.getBytes();
        try {
            client.getOutputStream().write(resultBytes);
            client.close();
        } catch (IOException e) {
            logger.info("Error in Server.684. "+e.getMessage(), e);
            e.printStackTrace();
        }
    }

    /**
     * 重放记录
     */
    private void replayRecord() {

        for(final Long pk : this.totalPkRecordMap.keySet()) {
            int[] metaArray = this.totalPkRecordMap.get(pk);
            Record record = getRecordFromMetaArray(pk, metaArray);
            this.finalRecordList.add(record);
        }
        // 不需要了
        this.totalPkRecordMap.clear();
        // 将Record根据pk排序
        Collections.sort(this.finalRecordList);
    }

    private String logRecord() {
        logger.info("Total pk num:"+this.totalPKNUM);
        StringBuilder sb = new StringBuilder(1024);
        for(final Record record : this.finalRecordList) {
            ArrayList<Column> sortColumnList = new ArrayList<>(record.getColumnNameSet().size());
            for(final String columnName : record.getColumnNameSet()) {
                sortColumnList.add(record.getColumn(columnName));
            }
            Collections.sort(sortColumnList);
            boolean flag = false;
            for(final Column column : sortColumnList) {
                if(flag) {
                    sb.append('\t');
                }else{
                    flag = true;
                }
                if(column.isStr) {
                    sb.append(column.strColumnVal);
                }else{
                    sb.append(column.longColumnVal);
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    // 当发生异常时，log 一段当前输入
    private void logInpuWhenError() {
//        if(this.mapFile != null) {
//            int curPos = this.mapFile.position();
//            logger.info("MapFile curpos:"+curPos);
//            // 往前回退1024个字符
//            int startPos = curPos - 1024;
//            if(startPos < 0) {
//                startPos = 0;
//            }
//            // 往后前进512个字符
//            int endPos = curPos + 512;
//            if(endPos > this.mapFile.limit()) {
//                endPos = this.mapFile.limit();
//            }
//            this.mapFile.position(startPos);
//            byte[] contents = new byte[endPos-startPos];
//            this.mapFile.get(contents);
//            String content = new String(contents);
//            logger.info(content);
//
//        }
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
        server.startServer(Constants.SERVER_PORT);
    }
}
