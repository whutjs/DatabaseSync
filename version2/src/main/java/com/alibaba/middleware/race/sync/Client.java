package com.alibaba.middleware.race.sync;

import com.sun.corba.se.spi.orbutil.fsm.Input;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import net.jpountz.lz4.*;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {
    static Logger logger = LoggerFactory.getLogger(Client.class);

    /** Socket */
    private Socket socket;

    private final static int port = Constants.SERVER_PORT;
    private static String ip;

    public static void main(String[] args) throws Exception {
        initProperties();
        logger.info("Welcome");
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.connect(ip, port);

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

        File resultFile = new File(Paths.get(Constants.RESULT_HOME, Constants.RESULT_FILE_NAME).toString());
        if(!resultFile.exists()) {
            try {
                resultFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileOutputStream fout = null;
        try {
            fout = new FileOutputStream(resultFile, false);
        } catch (FileNotFoundException e) {
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
        byte[] resultBytes = new byte[4096];
        try {
            socket.setSoTimeout(8*60*1000);
            System.out.println("IsConnected:"+this.socket.isConnected());
            logger.info("IsConnected:"+this.socket.isConnected());
            LZ4BlockInputStream in = new LZ4BlockInputStream(socket.getInputStream());
//            InputStream in = socket.getInputStream();
            int cnt = 0;
            while((cnt = in.read(resultBytes)) > 0) {
                fout.write(resultBytes, 0, cnt);
                totalBytesWrite += cnt;
            }
            System.out.println("totalBytesWrite:"+totalBytesWrite + "B "+ (totalBytesWrite/1024)+" KB");
            logger.info("totalBytesWrite:"+totalBytesWrite + "B "+ (totalBytesWrite/1024)+" KB");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("客户端异常:" + e.getMessage(), e);
        }finally{
            if(socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    socket = null;
                }
            }
            if(fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}