package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {
    //static Logger logger = LoggerFactory.getLogger(Client.class);

    /** Socket */
    private Socket socket;

    private final static int port = Constants.SERVER_PORT;
    private static String ip;

    public static void main(String[] args) throws Exception {
        //long startTime = System.currentTimeMillis();
        //initProperties();
        //logger.info("Welcome");
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.connect(ip, port);
        //long endTime = System.currentTimeMillis();
        //logger.info("Client运行时间:"+(endTime-startTime));
        //System.out.println("Client运行时间:"+(endTime-startTime));
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) {
        final int FILE_SIZE = 38334025;
        // final int FILE_SIZE = 9082741;
        File resultFile = new File(Paths.get(Constants.RESULT_HOME, Constants.RESULT_FILE_NAME).toString());
        if(!resultFile.exists()) {
            try {
                resultFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        MappedByteBuffer mapFile = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(resultFile, "rw");
            mapFile = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean connected = false;
        while(!connected) {
            try {
                socket = new Socket(host, port);
                connected = true;
            } catch (IOException e) {
                //e.printStackTrace();
                //logger.info("创建socket失败:" + e.getMessage(), e);
            }
        }
        int totalBytesWrite = 0;
        // 37MB,一次读完
        byte[] resultBytes = new byte[FILE_SIZE + 1024];
        try {
            //socket.setSoTimeout(8*60*1000);
            //logger.info("IsConnected:"+this.socket.isConnected());
//            LZ4BlockInputStream in = new LZ4BlockInputStream(socket.getInputStream());
            InputStream in = socket.getInputStream();
            int cnt = 0;
            while((cnt = in.read(resultBytes)) > 0) {
                mapFile.put(resultBytes, 0, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            //logger.info("客户端异常:" + e.getMessage(), e);
        }finally{
            if(socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    socket = null;
                }
            }
            if(raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}