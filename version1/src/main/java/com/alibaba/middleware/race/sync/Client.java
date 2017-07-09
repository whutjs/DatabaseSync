package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.net.Socket;
import java.nio.file.Paths;

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
    public void connect(String host, int port) throws Exception {
        File resultFile = new File(Paths.get(Constants.RESULT_HOME, Constants.RESULT_FILE_NAME).toString());
        if(!resultFile.exists()) {
            resultFile.createNewFile();
        }
        FileOutputStream fout = new FileOutputStream(resultFile, false);
        try {
            socket = new Socket(host, port);
            socket.setSoTimeout(5*60*1000);
            System.out.println("IsConnected:"+this.socket.isConnected());
            byte[] resultBytes = new byte[4096];
            InputStream socketInput = socket.getInputStream();
            int cnt = 0;
            while((cnt = socketInput.read(resultBytes)) > 0) {
                fout.write(resultBytes, 0, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("客户端异常:" + e.getMessage());
            logger.info("客户端异常:" + e.getMessage());
        }finally{
            if(socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    socket = null;
                }
            }
            fout.close();
        }

    }


}
