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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

// 我也不知道起什么名字好...
class PkUpdateRecord {
    // 变更类型
    public byte modifyType;
    // 变更后主键。该键用于索引pkRecordMap
    public Integer pkAfterModify;

}
/**
 * 用于多线程构建索引
 * Created by Jenson on 2017/6/10.
 */
public class Worker implements Runnable{
    Logger logger = LoggerFactory.getLogger(Server.class);

    private int arrayGrowCnt = 0;
    // 线程号，方便打log之类
    private final int workerNO;

    private HashMap<Integer, Record> workerResult;
    private HashMap<Integer, Integer> pkFinalVal;
    // 用于读数据
    private MappedByteBuffer mapFile;
    private RandomAccessFile dataRaf;
    // pkRecordMap的int[]的初始大小，需要根据数据量做调整
    private final int metaArrayInitSize = 4;
    // 用来存需要提取出字符串的bytes
    private byte[] lineBytes = new byte[256];
    private CountDownLatch latch;

    public Worker(final int workerNo, final HashMap<Integer, Record> result, final HashMap<Integer, Integer> pkFinalVal) {
        this.workerNO = workerNo;
        this.workerResult = result;
        this.pkFinalVal = pkFinalVal;
        // 开始解析文件
        final String fileName = Paths.get(Constants.MIDDLE_HOME, workerNO + ".txt").toString();
        File file = new File(fileName);
        if (!file.exists()) {
            logger.info(fileName + " doesnot exists");
            return;
        }
        try {
            // 原数据文件
            this.dataRaf = new RandomAccessFile(file, "r");
            final long len = dataRaf.length();
            this.mapFile = dataRaf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, len);
            logger.info(fileName+" size:"+len+" B " + (len/1024)+" KB");
        } catch (FileNotFoundException e) {
            logger.info(e.getMessage(), e);
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }
    }

    public void setCountDownLatch(final CountDownLatch lat) {
        this.latch = lat;
    }


    @Override
    public void run() {
        startParseFile();
        this.latch.countDown();
    }

    public void releaseResource() {
        this.workerResult = null;
        this.pkFinalVal = null;
        if(this.dataRaf != null) {
            try {
                this.dataRaf.close();
            } catch (IOException e) {
                logger.info(e.getMessage(), e);
            }
        }
        this.mapFile = null;
    }

    private void startParseFile() {
        this.mapFile.position(0);
        int prePos = 0, newPos = 0, len = 0;
        while(this.mapFile.hasRemaining()) {
            // 第一byte是‘I' 'U' 'D'
            final byte modifyType = this.mapFile.get();
            if(modifyType == Constants.INSERT_TYPE) {
                // 只有变更后列值
                int pkAfter = this.mapFile.getInt();
                // 接下来是数据的长度
                short dataLen = this.mapFile.getShort();
                Integer finalPk = this.pkFinalVal.get(pkAfter);
                if(finalPk == null) {
                    // 不在范围，跳过
                    this.mapFile.position(this.mapFile.position() + dataLen);
                }else{
                    // 在范围，构建record
                    Record record = this.workerResult.get(finalPk);
                    if(record == null) {
                        record = new Record(finalPk);
                        this.workerResult.put(finalPk, record);
                    }
                    this.mapFile.get(this.lineBytes, 0, dataLen);
                    //System.out.println("Insert "+finalPk+"　"+ new String(this.lineBytes, 0, dataLen));
                    ByteBuffer lineBuf = ByteBuffer.wrap(this.lineBytes, 0, dataLen);
                    replayModify(lineBuf, record, 0, pkAfter, modifyType);
                }
            }else if(modifyType == Constants.DELETE_TYPE) {
                // 只有变更前列值
                int pkBefore = this.mapFile.getInt();
//                // 接下来是数据的长度
//                short dataLen = this.mapFile.getShort();
                Integer finalPk = this.pkFinalVal.get(pkBefore);
                if(finalPk == null) {
                    // 不在范围，跳过
//                    this.mapFile.position(this.mapFile.position() + dataLen);
                }else{
                    // 在范围，构建record
                    Record record = this.workerResult.get(finalPk);
                    if(record == null) {
                        // 如果是空，是bug!!
                        logger.info("Error! DELETE workerResult doesnot have key:"+finalPk);
                        System.out.println("Error! DELETE workerResult doesnot have key:"+finalPk);
                        record = new Record(finalPk);
                        this.workerResult.put(finalPk, record);
                    }
//                    this.mapFile.get(this.lineBytes, 0, dataLen);
                    //System.out.println("Delete "+finalPk+"　"+ new String(this.lineBytes, 0, dataLen));
//                    ByteBuffer lineBuf = ByteBuffer.wrap(this.lineBytes, 0, dataLen);
                    replayModify(null, record, pkBefore, 0, modifyType);
                }
            }else if(modifyType == Constants.UPDATE_TYPE) {
                int pkBefore = this.mapFile.getInt();
                int pkAfter = this.mapFile.getInt();
                // 接下来是数据的长度
                short dataLen = this.mapFile.getShort();
                Integer finalPk = this.pkFinalVal.get(pkAfter);
                if(finalPk == null) {
                    // 不在范围，跳过
                    this.mapFile.position(this.mapFile.position() + dataLen);
                }else{
                    // 在范围，构建record
                    Record record = this.workerResult.get(finalPk);
                    if(record == null) {
                        // 如果是空，是bug!!
                        logger.info("Error! UPDATE workerResult doesnot have key:"+finalPk);
                        System.out.println("Error! UPDATE workerResult doesnot have key:"+finalPk);
                        record = new Record(finalPk);
                        this.workerResult.put(finalPk, record);
                    }
                    this.mapFile.get(this.lineBytes, 0, dataLen);
                    //System.out.println("Update "+finalPk+"　"+ new String(this.lineBytes, 0, dataLen));
                    ByteBuffer lineBuf = ByteBuffer.wrap(this.lineBytes, 0, dataLen);
                    replayModify(lineBuf, record, pkBefore, pkAfter, modifyType);
                }
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
        if(len > this.lineBytes.length ) {
            byte[] tmpBytes = new byte[len];
            dataBuf.get(tmpBytes);
            String longStr = new String(tmpBytes);
            logger.info("Found long str:len="+len + " str:"+longStr);
            return longStr;
        }else {
            // NOTE:读完之后，dataBuf再次指向的是schema后面的'|'!!!!
            dataBuf.get(this.lineBytes, 0, len);
            return new String(this.lineBytes, 0, len);
        }
    }

    private void replayModify(final ByteBuffer dataBuf, final Record record, final int pkBefore,
                              final int pkAfter, final byte modifyType) {
        // 格式：first_name:2:0|NULL|阮|last_name:2:0|NULL|明|sex:2:0|NULL|男|score:1:0|NULL|356|score2:1:0|NULL|74549|
        if(modifyType == Constants.DELETE_TYPE) {
            // 删除类型，直接清空column，然后返回
            record.clearColumns();
            return;
        }
        int preIdx = 0, idx = 0;
        // 表示列序号
        int columnNO = 0;
        // 循环提取列信息
        while(dataBuf.hasRemaining()) {
            // 提取列名
            byte b;
            idx = 0;
            while((b = dataBuf.get()) != ':') {
                this.lineBytes[idx++] = b;
            }
            final String columnName = new String(this.lineBytes, 0, idx);
            final byte valueType = dataBuf.get();
            // 跳过':'
            dataBuf.get();
            final byte isPK = dataBuf.get();
            // 现在dataBuf的位置是'|'，前进一位
            dataBuf.get();
            if(modifyType == Constants.INSERT_TYPE) {
                // 插入只提取变更后列值
                while(dataBuf.get() != '|') {

                }
                Column column = new Column();
                // 插入，需要记录列顺序
                column.columnNO = columnNO;
                if(valueType == '2') {
                    // 如果是字符串
                    idx = 0;
                    while ((b = dataBuf.get()) != '|') {
                        this.lineBytes[idx++] = b;
                    }
                    final String columnValue = new String(this.lineBytes, 0, idx);
                    column.isStr = true;
                    column.strColumnVal = columnValue;

                }else{
                    // 数字类型
                    long columnValue = 0;
                    while ((b = dataBuf.get()) != '|') {
                        columnValue = columnValue*10 + (b-'0');
                    }
                    // 数字类型，也就是Long
                    column.isStr = false;
                    column.longColumnVal = columnValue;
                }
                record.addColumn(columnName, column);
            }else if(modifyType == Constants.UPDATE_TYPE) {
                Column column = record.getColumn(columnName);
                if(valueType == '2') {
                    // 只提取变更后列值
                    while(dataBuf.get() != '|') {
                    }
                    idx = 0;
                    while ((b = dataBuf.get()) != '|') {
                        this.lineBytes[idx++] = b;
                    }
                    final String columnValueAfter = new String(this.lineBytes, 0, idx);

                    column.isStr = true;
                    column.strColumnVal = columnValueAfter;
                }else{
                    // 只提取变更后列值
                    while(dataBuf.get() != '|') {
                    }
                    // 数字类型
                    long columnValue = 0;
                    while ((b = dataBuf.get()) != '|') {
                        columnValue = columnValue*10 + (b-'0');
                    }
                    // 数字类型，也就是Long
                    column.isStr = false;
                    column.longColumnVal = columnValue;
                }
            }
            // skip '|'
            // dataBuf.get();
            ++columnNO;
        }

    }
}