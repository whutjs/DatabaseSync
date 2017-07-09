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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 服务器类
 * Created by Jenson on 2017/6/7.
 */
public class Server {
    // static Logger logger = LoggerFactory.getLogger(Server.class);

    // 统计时间
    private long constructPKChangedRecordTime = 0;


    private final int startPkId;
    private final int endPkId;


    private final int FILE_NO_LIMIT = Constants.THREAD_NUM;

    // 从原文件读取字节到lineBytes，一次处理一行
    private final byte[] lineBytes = new byte[128];
    // 最多只有400万，直接用数组
    private long[] recordMap;
    //    private HashMap<Integer, RecordMeta> recordMap;
    private HashMap<Integer, Integer> columnValStrMap;
    private ArrayList<String> columnValStrList;
    // 最大的主键的值不能超过这个
    private final int MAX_PK_VAL = 5240296;
    //跳过前面的binary id和timestamp
    private final int VALID_OFFSET = 34;
    // delete操作时可跳过的位数
    private final int DELETE_SKIP_OFFSET = 105;
    // 本地热身赛数据
    //private final int DELETE_SKIP_OFFSET = 86;
    // 当Insert的pk不在范围时，可以跳过的字节数
    private final int INSERT_NOT_IN_RANGE_SKIP_OFFSET = 101;
    // 本地热身赛数据
    //private final int INSERT_NOT_IN_RANGE_SKIP_OFFSET = 82;
    private final int skipLenWhenPkFirstByteNotEq = 8;
    //private final int skipLenWhenPkFirstByteNotEq = 7;

    private final int UpdateFileNO = 7;
    // private final int UpdateFileNO = 6;

    private final int updateFirstNameSkipLen = 22;
    private final int updateLastNameSkipLen = 21;
    private final int updateScoreSkipLen = 15;
    private final int replaySkipNumValLen = 2;

    private final int MIN_PK_INSERT_NOT_UPDATE = 5196013;
    private final int MAX_PK_INSERT_NOT_UPDATE = 5240295;

    private final int SKIP_LEN_UPDATE = 19;
    private final int SKIP_LEN_DELETE = 118;
    //private final int SKIP_LEN_DELETE = 97;
    private final int SKIP_LEN_INSERT = 125;
    //private final int SKIP_LEN_INSERT = 102;

    // 线上测试
    private final int[] fileInsertPos = {
            //   1.txt      2.txt    3.txt      4.txt       5.txt      6.txt
            0, 190168687, 871613619, 144538905, 915084867, 110899640, 101964297,
            //7.txt    8.txt       9.txt     10.txt
            396995614, 139844986,  80312750, 69785343
    };
    private final int[] fileInsertEndPos = {
            //null,      1.txt,    2.txt,      3.txt,      4.txt
            0,         1210082929, 871763998, 1022702547, 988927221,
            // 5.txt   6.txt      7.txt      8.txt      9.txt       10.txt
            875937219, 730158141, 998139701, 516414592, 1023618879, 1095826428
    };
    /*
    // 本地测试
    private final int[] fileInsertPos = {
            //   1.txt      2.txt    3.txt      4.txt       5.txt      6.txt
            0, 16538238, 0, 0, 0, 0, 0,
            //7.txt   8.txt   9.txt   10.txt
            0,    0,      0,       0
    };
    private final int[] fileInsertEndPos = {
            //null,      1.txt,    2.txt,      3.txt,      4.txt
            0,         333882633, 0, 0, 0,
            // 5.txt   6.txt      7.txt      8.txt      9.txt       10.txt
            0, 0, 0, 0, 0, 0
    };
    */

    // 线上测试
    private final int[] fileUpdatePos = {
            // null, 1.txt  2  3  4 5   6.txt  7.txt
            0,   0,     0, 0, 0, 0, 0,     494036530,
            //  8.txt       9.txt,       10.txt
            239293524,  848962916,   20470450
    };
    // 线上测试
    private final int[] fileUpdateEndPos = {
            // null, 1.txt  2  3  4 5   6.txt       7.txt
            0,   0,     0, 0, 0, 0,     0,       994551876,
            //  8.txt    9.txt,        10.txt
            515326120,  918191418,   1095027772
    };
    /*
    // 本地测试
    private final int[] fileUpdatePos = {
        // null, 1.txt  2  3  4 5   6.txt       7.txt
            0,   0,     0, 0, 0, 0, 445750042,  0,
        //  8.txt       9.txt, 10.txt
            560697856,  214,   130072089
    };
    // 本地测试
   private final int[] fileUpdateEndPos = {
            // null, 1.txt  2  3  4 5   6.txt       7.txt
            0,   0,     0, 0, 0, 0, 698716667,  0,
            //  8.txt    9.txt,        10.txt
            1172983393,  1541731204,   415286862
    };
     */
    // 线上测试
    private final int[] fileDeletePos = {
            //null, 1.txt 2, 3, 4, 5, 6, 7.txt
            0,      0,    0, 0, 0, 0, 0, 399096811,
            //8.txt   9.txt        10.txt
            34801698, 1023619262, 1031143113
    };
    private final int[] fileDeleteEndPos = {
            //null, 1.txt 2, 3, 4, 5, 6, 7.txt
            0,      0,    0, 0, 0, 0, 0, 945760890,
            //8.txt   9.txt        10.txt
            516548265, 1023794262, 1092769278
    };
    /*
     // 本地测试
     private final int[] fileDeletePos = {
            //null, 1.txt 2, 3, 4, 5, 6.txt,     7.txt
            0,      0,    0, 0, 0, 0, 265901951, 0,
            //8.txt   9.txt        10.txt
            279397553, 0,          0
    };
    private final int[] fileDeleteEndPos = {
            //null, 1.txt 2, 3, 4, 5, 6.txt,     7.txt
            0,      0,    0, 0, 0, 0, 278104039, 0,
            //8.txt   9.txt        10.txt
            282200413, 0,          0
    };
     */

    // 线上测试
    private final int[] fileStartPos = {
            //null, 1.txt 2, 3, 4, 5, 6, 7.txt
            0,      0,    0, 0, 0, 0, 0, 396995614,
            //8.txt   9.txt        10.txt
            34801698, 80312750, 20470450
    };
    private final int[] fileEndPos = {
            //null, 1.txt 2, 3, 4, 5, 6, 7.txt
            0,      0,    0, 0, 0, 0, 0, 998139701,
            //8.txt   9.txt        10.txt
            516548265, 1023794262, 1095826428
    };
    /*
    // 本地测试
    private final int[] fileStartPos = {
            //null, 1.txt 2, 3, 4, 5, 6.txt,     7.txt
            0,      0,    0, 0, 0, 0, 265901951, 0,
            //8.txt   9.txt   10.txt
            279397553, 214,  130072089
    };
    private final int[] fileEndPos = {
            //null, 1.txt 2, 3, 4, 5, 6.txt,     7.txt
            0,      0,    0, 0, 0, 0, 698716667, 0,
            //8.txt      9.txt        10.txt
            1172983393, 1541731204, 415286862
    };
    */

    // 线上
    private final int[][] fileInsertStartPosRange = {
            {0},
            // 1.txt
            {190168687, 1209620622},
            // 2.txt
            {871613619},
            // 3.txt
            {144538905, 423146974, 883427898, 972076658, 1022533638},
            // 4.txt
            {915084867, 988655590},
            // 5.txt
            {110899640, 416997245, 602147757, 874593208},
            // 6.txt
            {101964297, 172466384, 252198194, 443088339, 667061278, 729520128}
    };
    private final int[][] fileInsertEndPosRange = {
            {0},
            // 1.txt
            {955283989, 1210082929},
            // 2.txt
            {871763998},
            // 3.txt
            {146247726, 424548708, 885313324, 973194383, 1022702547},
            // 4.txt
            {916175582, 988927221},
            // 5.txt
            {112443680, 418198801, 603643496, 875937219},
            // 6.txt
            {102864032, 172606321, 252725774, 444671370, 668942438, 730158141}
    };
    /*
    // 本地测试
    private final int[][] fileInsertStartPosRange = {
            {0},
            // 1.txt
            {16538238},
            // 2.txt
            {0},
            // 3.txt
            {0},
            // 4.txt
            {0},
            // 5.txt
            {0},
            // 6.txt
            {0}
    };
    private final int[][] fileInsertEndPosRange = {
            {0},
            // 1.txt
            {333882633},
            // 2.txt
            {0},
            // 3.txt
            {0},
            // 4.txt
            {0},
            // 5.txt
            {0},
            // 6.txt
            {0}
    };
    */
    // 把socket也放到Server类
    private Socket client;
    private ServerSocket serverSocket;
    private final CountDownLatch socketLatch;

    public Server(final String[] args) {
        /**
         * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
         * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
         * id>100 and id<200
         */
        // 第一个参数是Schema Name
        //this.schema = args[0];
        // 第二个参数是table Name
        //this.tableName = args[1];
        // 第三个参数是start pk Id
        this.startPkId = Integer.parseInt(args[2]);
        // 第四个参数是end pk Id
        int endpk = Integer.parseInt(args[3]);
        this.endPkId = (endpk < MAX_PK_VAL? endpk:MAX_PK_VAL);

        this.columnValStrMap = new HashMap<>(4096);
        this.columnValStrList = new ArrayList<>(1925);
        this.recordMap = new long[(this.endPkId - this.startPkId + 10)>>1];

        this.socketLatch = new CountDownLatch(1);
    }

    public void startServer(final int port) throws InterruptedException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverSocket = new ServerSocket(port);
                    client = serverSocket.accept();
                    //logger.info("IsConnected:"+client.isConnected());
                } catch (IOException e) {
                    //logger.info("服务器异常: " + e.getMessage(), e);
                }
                socketLatch.countDown();
            }
        }).start();
        //long startTime = System.currentTimeMillis();
        constructPKChangedRecord();
        //long endTime = System.currentTimeMillis();
        //this.constructPKChangedRecordTime = (endTime-startTime);

        sendLogMsg2Client();
    }

    private void parseFileForInsertOnly(final int curFileNo, final MappedByteBuffer mapFile) {
        mapFile.position(mapFile.position() + this.fileInsertPos[curFileNo]);
        final int[] insertStartPosRange = this.fileInsertStartPosRange[curFileNo];
        final int[] insertEndPosRange = this.fileInsertEndPosRange[curFileNo];
        final int length = insertStartPosRange.length;
        for(int i = 0; i < length; ++i) {
            final int startPos = insertStartPosRange[i];
            final int endPos = insertEndPosRange[i];
            mapFile.position(startPos);
            while(mapFile.hasRemaining()) {
                final int fileCurPos = mapFile.position();
                if(fileCurPos > endPos) {
                    break;
                }
                mapFile.position(fileCurPos+VALID_OFFSET);
                // 读了1个'|'后就是schema的第一个字符
                while(mapFile.get() != 124) {
                    // '|'的ASCII值是124
                }
                mapFile.position(mapFile.position()+Constants.SKIP_POS);
                // 下一个字符就是'I U D'了
                final byte modifiedType = mapFile.get();
                // 读掉后面的'|'
                mapFile.get();
//                // 或者直接skip
//                mapFile.position(mapFile.position()+1);
                if(modifiedType == 73) {
                    // 'I'的ASCII值是73
                    extractPKChangedInfo(mapFile, Constants.INSERT_TYPE);
                }else if(modifiedType == 85){
                    // 'U'的ASCII值是85
                    mapFile.position(mapFile.position()+this.SKIP_LEN_UPDATE);
                    // '\n'的ASCII值是10
                    while(mapFile.get() != 10) {
                    }
                }
                else if(modifiedType == 68) {
                    // 'D'的ASCII值是68
                    mapFile.position(mapFile.position()+this.SKIP_LEN_DELETE);
                    // '\n'的ASCII值是10
                    while(mapFile.get() != 10) {
                    }
                }
            }
        }
    }

    private void constructPKChangedRecord() {
        int curFileNo = 1;
        while(curFileNo <= this.FILE_NO_LIMIT) {
            // 开始解析文件
            final String fileName = Paths.get(Constants.DATA_HOME, curFileNo + ".txt").toString();
            File file = new File(fileName);
            // 原数据文件
            RandomAccessFile dataRaf = null;
            MappedByteBuffer mapFile = null;
            try {
                // 原数据文件
                dataRaf = new RandomAccessFile(file, "r");
                mapFile = dataRaf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, dataRaf.length());
            } catch (FileNotFoundException e) {
                //logger.info(e.getMessage(), e);
            } catch (IOException e) {
                //logger.info(e.getMessage(), e);
            }
            if(curFileNo < this.UpdateFileNO) {
                parseFileForInsertOnly(curFileNo, mapFile);
                if(dataRaf != null) {
                    try {
                        dataRaf.close();
                    } catch (IOException e) {
                        //logger.info(e.getMessage(), e);
                    }
                }
                ++curFileNo;
                continue;
            }
            mapFile.position(mapFile.position() + this.fileStartPos[curFileNo]);
            while(mapFile.hasRemaining()) {
                final int fileCurPos = mapFile.position();
                if(fileCurPos > this.fileEndPos[curFileNo]) {
                    break;
                }
                // 线上测试
                final int targetFile7StartPos = 591597650;
                final int targetFile7EndPos = 838155519;

                final int targetFile9StartPos = 82212546;
                final int targetFile9EndPos = 717988084;
                if(curFileNo == 7) {
                    if(fileCurPos > targetFile7StartPos && fileCurPos < targetFile7EndPos) {
                        // skip
                        mapFile.position(targetFile7EndPos);
                        continue;
                    }
                }
                else if(curFileNo == 9) {
                    if(fileCurPos > targetFile9StartPos && fileCurPos < targetFile9EndPos) {
                        // skip
                        mapFile.position(targetFile9EndPos);
                        continue;
                    }
                }
                // 开始解析
                mapFile.position(fileCurPos+VALID_OFFSET);
                // 读了1个'|'后就是schema的第一个字符
                while(mapFile.get() != 124) {
                    // '|'的ASCII值是124
                }
                mapFile.position(mapFile.position()+Constants.SKIP_POS);
                // 下一个字符就是'I U D'了
                final byte modifiedType = mapFile.get();
                // 读掉后面的'|'
                mapFile.get();
//                // 或者直接skip
//                mapFile.position(mapFile.position()+1);
                if(modifiedType == 73) {
                    // 'I'的ASCII值是73
                    // 根据范围来insert
                    if(fileCurPos < this.fileInsertPos[curFileNo] || fileCurPos > this.fileInsertEndPos[curFileNo]) {
                        mapFile.position(mapFile.position() + this.SKIP_LEN_INSERT);
                        // '\n'的ASCII值是10
                        while(mapFile.get() != 10) {
                        }
                    }else{
                        extractPKChangedInfo(mapFile, Constants.INSERT_TYPE);
                    }
                }else if(modifiedType == 85){
                    // 'U'的ASCII值是85
                    // extractPKChangedInfo(mapFile, Constants.UPDATE_TYPE, fileCurPos);
                    if(fileCurPos < this.fileUpdatePos[curFileNo] ||
                            fileCurPos > this.fileUpdateEndPos[curFileNo]) {
                        // skip
                        mapFile.position(mapFile.position()+this.SKIP_LEN_UPDATE);
                        // '\n'的ASCII值是10
                        while(mapFile.get() != 10) {
                        }
                    }else {
                        extractPKChangedInfo(mapFile, Constants.UPDATE_TYPE);
                    }
                }
                else if(modifiedType == 68) {
                    // 'D'的ASCII值是68
                    if(fileCurPos < this.fileDeletePos[curFileNo] ||
                            fileCurPos > this.fileDeleteEndPos[curFileNo]) {
                        mapFile.position(mapFile.position() + this.SKIP_LEN_DELETE);
                        // '\n'的ASCII值是10
                        while(mapFile.get() != 10) {
                        }
                    }else {
                        extractPKChangedInfo(mapFile, Constants.DELETE_TYPE);
                    }
                }
            }
            if(dataRaf != null) {
                try {
                    dataRaf.close();
                } catch (IOException e) {
                    //logger.info(e.getMessage(), e);
                }
            }
            ++curFileNo;
        }
    }




    private void extractPKChangedInfo(final ByteBuffer dataBuf, final byte modifiedType) {
        // 格式：id:1:1|6577137|6577137|......
        // 提取出PK的变更信息
        while(true) {
            // skip到主键部分内容
            // 直接跳过"id:1:1|" 7个byte
            dataBuf.position(dataBuf.position() + 7);
            int pkBefore = 0;
            int pkAfter = 0;
            if(modifiedType == Constants.INSERT_TYPE) {
                // 只有变更后的值
                // skip变更前的值
                // NULL| 是5个byte
                dataBuf.position(dataBuf.position()+5);
                // 计算变更后的值
                byte b;
                while((b = dataBuf.get()) != 124) {
                    // '|'的ASCII值是124
                    pkAfter = pkAfter*10 + (b-48);
                }
                // 如果不在范围，skip掉
                if(((pkAfter & 1) == 0)
                        || pkAfter <= this.startPkId
                        || pkAfter >= this.endPkId) {
                    dataBuf.position(dataBuf.position() + this.INSERT_NOT_IN_RANGE_SKIP_OFFSET);
                    // '\n'的ASCII值是10
                    while(dataBuf.get() != 10) {
                    }
                    return;
                }
                final int pkIdx =  (pkAfter-this.startPkId)>>1;
                long record = replayModify(dataBuf, 0, modifiedType);
                this.recordMap[pkIdx] = record;
            }else if(modifiedType == Constants.DELETE_TYPE) {
                // 只有变更前的值
                byte b;
                while((b = dataBuf.get()) != 124) {
                    // '|'的ASCII值是124
                    pkBefore = pkBefore*10 + (b-48);
                }
                if((pkBefore&1) == 1 && (pkBefore > this.startPkId && pkBefore < this.endPkId)) {
                    final int pkIdx =  (pkBefore-this.startPkId)>>1;
                    this.recordMap[pkIdx] = 0;
                }
                // 直接跳过剩余的byte
                dataBuf.position(dataBuf.position() + this.DELETE_SKIP_OFFSET);
                // '\n'的ASCII值是10
                while(dataBuf.get() != 10) {
                }

            }else if(modifiedType == Constants.UPDATE_TYPE) {
                byte b;
                byte pkBeforeFirstByte = dataBuf.get();
                pkBefore = pkBeforeFirstByte-48;
                while((b = dataBuf.get()) != 124) {
                    // '|'的ASCII值是124
                    pkBefore = pkBefore*10 + (b-48);
                }
                // 如果不在范围，skip掉
                if(((pkBefore & 1) == 0)
                        || pkBefore <= this.startPkId
                        || pkBefore >= this.endPkId) {
                    // 先Skip PkAfter
                    dataBuf.position(dataBuf.position() + 1);
                    // '|'的ASCII值是124
                    while(dataBuf.get() != 124) {
                    }
                    b = dataBuf.get();
                    // '\n'的ASCII值是10
                    if(b == 10) {
                        return;
                    }else {
                        // 'f'的ASCII值是102
                        if(b == 102) {
                            // firstName
                            dataBuf.position(dataBuf.position()+this.updateFirstNameSkipLen);
                        }else if(b == 108) {
                            // 'l'的ASCII值是108
                            // last_name
                            dataBuf.position(dataBuf.position()+this.updateLastNameSkipLen);
                        }else if(b == 115) {
                            // 's'的ASCII值是115
                            //score, score2, sex
                            dataBuf.position(dataBuf.position()+this.updateScoreSkipLen);
                        }
                        // '\n'的ASCII值是10
                        while(dataBuf.get() != 10) {
                        }
                    }
                    return;
                }
                byte pkAfterFirstByte = dataBuf.get();
                pkAfter = pkAfterFirstByte-48;
                if(pkAfterFirstByte != pkBeforeFirstByte) {
                    dataBuf.position(dataBuf.position()+this.skipLenWhenPkFirstByteNotEq);
                    // 直接过滤掉
                    // '\n'的ASCII值是10
                    while(dataBuf.get() != 10) {
                    }
                    final int pkIdx =  (pkBefore-this.startPkId)>>1;
                    // 并且把原来的也删除掉
                    this.recordMap[pkIdx] = 0;
                    return;
                }
                // '|'的ASCII值是124
                while((b = dataBuf.get()) != 124) {
                    pkAfter = pkAfter*10 + (b-48);
                }
                // 如果不在范围，skip掉
                if(((pkAfter & 1) == 0) || pkAfter <= this.startPkId
                        || pkAfter >= this.endPkId) {
                    // '\n'的ASCII值是10
                    while(dataBuf.get() != 10) {
                    }
                    final int pkIdx =  (pkBefore-this.startPkId)>>1;
                    // 并且把原来的也删除掉
                    this.recordMap[pkIdx] = 0;
                    return;
                }
                final int pkIdx = (pkBefore-this.startPkId)>>1;
                long record = this.recordMap[pkIdx];
                record = replayModify(dataBuf, record, modifiedType);
                this.recordMap[pkIdx] = record;
            }
            return;
        }
    }

    private long replayModify(final ByteBuffer dataBuf, long record, final byte modifyType) {
        int preIdx = 0, idx = 0;
        // 循环提取列信息
        while(true) {
            // 提取列名
            byte b = dataBuf.get();
            // '\n'的ASCII值是10
            if(b == 10) {
                break;
            }
            final int prePos = dataBuf.position();
            // 'f'的ASCII值是102
            if(b == 102) {
                // firstName
                dataBuf.position(dataBuf.position()+9);
            }else if(b == 108) {
                // 'l'的ASCII值是108
                // lastName
                dataBuf.position(dataBuf.position()+8);
            }else if(b == 115) {
                // 's'的ASCII值是115
                if(modifyType == Constants.INSERT_TYPE) {
                    b = dataBuf.get();
                    if(b == 99) {
                        // 'c'的ASCII值是115
                        // score, score2
                        dataBuf.position(dataBuf.position()+3);
                    }
                }else{
                    dataBuf.position(dataBuf.position()+4);
                }
            }
            // ':'的ASCII值是58
            while(dataBuf.get() != 58) {
            }
            final int columnName = dataBuf.position()-prePos;
            // skip剩余4byte
            dataBuf.position(dataBuf.position()+4);
            if(modifyType == Constants.INSERT_TYPE) {
                // NULL| 是5个byte
                dataBuf.position(dataBuf.position()+5);
                if(columnName == 3 || columnName == 9 || columnName == 10) {
                    // 3,9,10是字符串
                    idx = 0;
                    // '|'的ASCII值是124
                    while ((b = dataBuf.get()) != 124) {
                        this.lineBytes[idx++] = b;
                    }
                    final int columnStrHashVal = byteArrayHashCode(this.lineBytes, idx);
                    Integer strIdx = this.columnValStrMap.get(columnStrHashVal);
                    if(strIdx == null) {
                        // 还没有，只能new String然后加进去了
                        final String columnValue = new String(this.lineBytes, 0, idx);
                        this.columnValStrList.add(columnValue);
                        strIdx = this.columnValStrList.size() - 1;
                        this.columnValStrMap.put(columnStrHashVal, strIdx);
                    }
                    if(columnName == 3) {
                        long sex = 0;
                        if(columnStrHashVal == 946) {
                            // "男"的byte哈希值是2345
                            // "女"的bytes哈希值是946
                            sex = 1;
                        }
                        record = setSex(record, sex);
                    }else {
                        if (columnName == 10) {
                            record = setFirstName(record, strIdx.longValue());
                        } else if (columnName == 9) {
                            record = setLastName(record, strIdx.longValue());
                        }
                    }
                }else{
                    // 数字类型
                    long columnValue = 0;
                    // '|'的ASCII值是124
                    while ((b = dataBuf.get()) != 124) {
                        columnValue = columnValue*10 + (b-48);
                    }
                    if (columnName == 5) {
                        record = setScore(record, columnValue);
                    } else if (columnName == 6) {
                        record = setScore2(record, columnValue);
                    }
                }

            }else if(modifyType == Constants.UPDATE_TYPE) {
                if(columnName == 3 || columnName == 9 || columnName == 10) {
                    // 3,9,10是字符串
                    // 只提取变更后列值
                    // '|'的ASCII值是124
                    while(dataBuf.get() != 124) {
                    }
                    idx = 0;
                    // '|'的ASCII值是124
                    while ((b = dataBuf.get()) != 124) {
                        this.lineBytes[idx++] = b;
                    }
                    final int columnStrHashVal = byteArrayHashCode(this.lineBytes, idx);
                    Integer strIdx = this.columnValStrMap.get(columnStrHashVal);
                    if(strIdx == null) {
                        // 还没有，只能new String然后加进去了
                        final String columnValue = new String(this.lineBytes, 0, idx);
                        this.columnValStrList.add(columnValue);
                        strIdx = this.columnValStrList.size() - 1;
                        this.columnValStrMap.put(columnStrHashVal, strIdx);
                    }
                    if(columnName == 3) {
                        long sex = 0;
                        if(columnStrHashVal == 946) {
                            sex = 1;
                        }
                        record = setSex(record, sex);
                    }else {
                        if (columnName == 10) {
                            record = setFirstName(record, strIdx.longValue());
                        } else if (columnName == 9) {
                            record = setLastName(record, strIdx.longValue());
                        }
                    }
                }else{
                    dataBuf.position(dataBuf.position()+2);
                    // 只提取变更后列值
                    // '|'的ASCII值是124
                    while(dataBuf.get() != 124) {
                    }
                    // 数字类型
                    long columnValue = 0;
                    // '|'的ASCII值是124
                    while ((b = dataBuf.get()) != 124) {
                        columnValue = columnValue*10 + (b-48);
                    }
                    if (columnName == 5) {
                        record = setScore(record, columnValue);
                    } else if (columnName == 6) {
                        record = setScore2(record, columnValue);
                    }
                }
                record = setValidBit(record);
            }
        }
        return record;
    }

    private int byteArrayHashCode(byte[] a, final int len) {
        int result = 1;
        for(int i = 0; i < len; ++i) {
            result = 31 * result + a[i];
        }
        return result;
    }
    // 将主键的值填到char[]
    private int convertPk2Char(int pk, char[] result, int offset) {
        // 线上测评的pk的位数是7位
        int pkLen = 7;
//        // 本地测评范围是6~7位
//        if(pk < 1000000) {
//            pkLen = 6;
//        }else{
//            pkLen = 7;
//        }
        int endIdx = offset+pkLen-1;
        while(pk > 0) {
            // '0'的值是48
            result[endIdx--] = (char)((pk%10)+ 48);
            pk /= 10;
        }
        return offset+pkLen;
    }

    private int convertScore2Char(long score, char[] result, int offset) {
        int scoreLen;
        // 线上：Min score:40 max score:300039
        if(score < 100) {
            scoreLen = 2;
        }else if(score < 1000) {
            scoreLen = 3;
        }else if(score < 10000) {
            scoreLen = 4;
        }else if(score < 100000) {
            scoreLen = 5;
        }else{
            scoreLen = 6;
        }
        int endIdx = offset+scoreLen-1;
        while(score > 0) {
            // '0'的值是48
            result[endIdx--] = (char)((score%10)+ 48);
            score /= 10;
        }
        return offset + scoreLen;
    }

    // 简单的将结果字符串写到socket里面去
    private void sendLogMsg2Client() {
        int pk = this.startPkId + 1;
        final int END_PK = this.endPkId;
        int pknum = 0;
        // 自己手动模拟StringBuilder
        char[] results = new char[38334025+10];
        int resultIdx = 0;
        for(; pk < END_PK; pk += 2) {
            final int pkIdx = (pk-this.startPkId)>>1;
            long record = this.recordMap[pkIdx];
            if(record == 0) {
                continue;
            }
            if(!isValid(record)) {
                if(pk >= this.MIN_PK_INSERT_NOT_UPDATE
                        && pk <= this.MAX_PK_INSERT_NOT_UPDATE) {
                }else{
                    continue;
                }
            }
            // 先放主键
            resultIdx = convertPk2Char(pk, results, resultIdx);
            results[resultIdx++] = '\t';
            // first_name
            int valIdx = (int)extractFirstName(record);
            final String firstName = this.columnValStrList.get(valIdx);
            for(int i = 0; i < firstName.length(); ++i) {
                results[resultIdx++] = firstName.charAt(i);
            }
            results[resultIdx++] = '\t';
            // last_name
            valIdx = (int)extractLastName(record);
            final String lastName = this.columnValStrList.get(valIdx);
            for(int i = 0; i < lastName.length(); ++i) {
                results[resultIdx++] = lastName.charAt(i);
            }
            results[resultIdx++] = '\t';
            // sex
            int sexBit = (int)extractSex(record);
            if(sexBit == 0) {
                results[resultIdx++] = '男';
            }else{
                results[resultIdx++] = '女';
            }
            results[resultIdx++] = '\t';
            // score
            long score = extractScore(record);
            resultIdx = convertScore2Char(score, results, resultIdx);
            results[resultIdx++] = '\t';
            // score2
            score = extractScore2(record);
            resultIdx = convertScore2Char(score, results, resultIdx);
            results[resultIdx++] = '\n';
        }
        this.recordMap = null;
        try {
            this.socketLatch.await();
        } catch (InterruptedException e) {
            //logger.info(e.getMessage(), e);
        }

        try {
            this.client.getOutputStream().write(new String(results, 0, resultIdx).getBytes());
            this.client.getOutputStream().flush();
            this.client.getOutputStream().close();
            this.client.close();
        } catch (IOException e) {
            //logger.info(e.getMessage(), e);
        }

//        StringBuilder logSb = new StringBuilder();
//        logSb.append("Construct time=").append(this.constructPKChangedRecordTime).append("\n");
//        logSb.append("\nInsert file pos:\n");
//        for(int i = 1; i < 11; ++i) {
//            logSb.append("File").append(i).append(" Pos range:\n");
//            ArrayList<Integer> filePosRange = resultPosList.get(i);
//            for(final Integer pos : filePosRange) {
//                logSb.append(pos).append('\n');
//            }
//        }
//
//        logger.info(logSb.toString());
//        System.out.println(logSb.toString());
    }

    /********** 位操作部分 ****************/
    /**
     * 63|62       51|50     39|38|37  19|18    0
     * +---------------------------------------+
     * |0|first_name|last_name|sex|score|score2|
     * +--------------------------------------+
     */
    // 12bit掩码
    private  final long BITMASK_12 = 0xfff;
    private  final long BITMASK_19 = 0x7ffff;

    private  long extractScore2(long record) {
        return (record & BITMASK_19);
    }

    private  final long SETSCORE2_MASK = (~BITMASK_19);
    private  long setScore2(long record, long score2) {
        record &= SETSCORE2_MASK;
        return (record | score2);
    }

    private  long extractScore(long record) {
        return ((record>>>19) & BITMASK_19);
    }

    private  final long SETSCORE_MASK = ~(BITMASK_19 << 19);
    private  long setScore(long record, long score) {
        record &= SETSCORE_MASK;
        return (record | (score<<19));
    }

    private  long extractSex(long record) {
        return ((record>>>38) & 1);
    }

    private  final long SETSEX_MASK = ~(1 << 38);
    private  long setSex(long record, long sex) {
        record &= SETSEX_MASK;
        return (record | (sex<<38));
    }

    private  long extractLastName(long record) {
        return ((record>>>39) & BITMASK_12);
    }

    private  final long SETLASTNAME_MASK = (~(BITMASK_12 << 39));
    private  long setLastName(long record, long lastName) {
        record &= SETLASTNAME_MASK;
        return (record | (lastName<<39));
    }

    private  long extractFirstName(long record) {
        return ((record>>>51) & BITMASK_12);
    }

    private  final long SETFIRSTNAME_MASK = (~(BITMASK_12 << 51));
    private  long setFirstName(long record, long firstName) {
        record &= SETFIRSTNAME_MASK;
        return (record | (firstName<<51));
    }
    private final long SET_VALID_BIT_MASK = (1L<<63);
    private long setValidBit(final long record) {
        // 设置最高位为有效。由于没有clearValidBit操作，所以直接设置为1即可
        return (record | SET_VALID_BIT_MASK);
    }
    private boolean isValid(long record) {
        return (((record>>63)&1) == 1);
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
        //logger.info("com.alibaba.middleware.race.sync.Server is running....");
        //long startTime = System.currentTimeMillis();
        //initProperties();
        Server server = new Server(args);
        server.startServer(Constants.SERVER_PORT);
        //long endTime = System.currentTimeMillis();
        //logger.info("Server运行时间:"+(endTime-startTime));
//        System.out.println("Server运行时间:"+(endTime-startTime));
    }
}