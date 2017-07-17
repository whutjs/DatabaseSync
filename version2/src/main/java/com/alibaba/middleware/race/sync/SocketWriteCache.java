package com.alibaba.middleware.race.sync;

import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单独起一条线程压缩+写socket
 * Created by Jenson on 2017/6/12.
 */
public class SocketWriteCache {
    static Logger logger = LoggerFactory.getLogger(Server.class);
    /** Socket */
    private Socket client;
    private ServerSocket serverSocket;
    private final int PORT;
    /**
     * 当一个cache正在压缩并写入socket时，另一个cache可以用来缓存Server生产出来的数据
     * 把SocketWriteCache的线程看成是消费者，Server线程是生产者，那么
     * SocketWriteCache的线程等待的条件是notEmpty，
     * Server线程等待的条件是notFull;
     */
    // 使用ByteBuffer，自带记录位置
    private ByteBuffer[] caches;
    // 表示当前用于给Producer写入的Cache
    private ByteBuffer curCache;
    // 一个cache 512KB
    private final int CACHE_SIZE = 512*1024;
    private final Lock lock;
    // 表示是否有待写入文件的cache
    private final Condition notEmpty;
    // 表示是否有空闲的cache供Server写入
    private final Condition notFull;
    // 表示作为消费者的SocketWriteCache的线程当前读的cache的位置
    private int readPtr;
    // 表示生产者线程当前生产的位置
    private int writePtr;
    // 当readPtr == writePtr时，说明是Empty,当前没有数据供SocketWriteCache消费写入文件;
    // 当readPtr != writePtr时，说明是Full，需要等待notFull

    public SocketWriteCache(final int port) {
        this.PORT = port;
        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.notFull = this.lock.newCondition();
        this.readPtr = this.writePtr = 0;
        this.caches = new ByteBuffer[2];
        this.caches[0] = ByteBuffer.allocate(CACHE_SIZE);
        this.caches[1] = ByteBuffer.allocate(CACHE_SIZE);
        this.curCache =this.caches[this.writePtr];
        new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        start();
                    }
                }
        ).start();
    }

    private void start() {
        try {
            serverSocket = new ServerSocket(this.PORT);
            client = serverSocket.accept();
            logger.info("IsConnected:"+this.client.isConnected());
        } catch (IOException e) {
            logger.info("服务器异常: " + e.getMessage(), e);
        }
        LZ4BlockOutputStream lz4Out = null;
        try {
            lz4Out = new LZ4BlockOutputStream(client.getOutputStream());
        } catch (IOException e) {
            logger.info("SocketWriteCache Error: " + e.getMessage(), e);
        }
//        OutputStream out = null;
//        try {
//            out = client.getOutputStream();
//        } catch (IOException e) {
//            logger.info("SocketWriteCache Error: " + e.getMessage(), e);
//        }
        while(true) {
            this.lock.lock();
            try{
                // FileWriteCache线程是消费者，需要notEmpty条件满足
                while(readPtr == writePtr) {
                    // 没有数据可供消费（可供写入文件）,说明是empty的，那么就
                    // 等待notEmpty条件为true
                    notEmpty.await();
                }
                // 写入数据
                ByteBuffer cache = this.caches[readPtr];
                cache.flip();
                if(cache.hasRemaining()) {
                    lz4Out.write(cache.array(), 0, cache.remaining());
                }
                cache.clear();
                // 前进一位
                readPtr = (readPtr+1)&1;
                // 通知有空闲cache了
                notFull.signal();
                if(writePtr < 0) {
                    // 说明写完数据了
                    break;
                }
            } catch (InterruptedException e) {
                logger.info("SocketWriteCache Error: " + e.getMessage(), e);
            } catch (IOException e) {
                logger.info("SocketWriteCache Error: " + e.getMessage(), e);
            } finally {
                this.lock.unlock();
            }
        }
        try {
            lz4Out.flush();
            lz4Out.close();
            client.close();
        } catch (IOException e) {
            logger.info("SocketWriteCache Error: " + e.getMessage(), e);
        }

    }

    public void writeData(final byte[] data, final int len) {
        // 5/30 21:30 Update:使用两个cache 两条线程
        if (len > this.curCache.remaining()) {
            // 如果当前cache满了，切换
            // 该方法在Server线程运行；属于生产者，需要notFull条件满足
            try {
                this.lock.lock();
                this.curCache = null;
                while (this.writePtr != this.readPtr) {
                    // 如果writePtr前进之后和readPtr相等，那么说明满了
                    // 等待notFull
                    this.notFull.await();
                }
                this.writePtr = (this.writePtr + 1) & 1;
                this.curCache = this.caches[this.writePtr];
                this.notEmpty.signal();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            } finally {
                this.lock.unlock();
            }
        }
        this.curCache.put(data, 0, len);
    }

    public void close() {
        this.writePtr = -1;
    }

    public void flush() {
        if(this.curCache.position() > 0) {
            // 强行切换一块
            try{
                this.lock.lock();
                this.curCache = null;
                while(this.writePtr != this.readPtr) {
                    // 如果writePtr前进之后和readPtr相等，那么说明满了
                    // 等待notFull
                    this.notFull.await();
                }
                this.writePtr = (this.writePtr+1)&1;
                this.curCache = this.caches[this.writePtr];
                this.notEmpty.signal();
            }catch(InterruptedException ex) {
                ex.printStackTrace();
            }finally {
                this.lock.unlock();
            }
        }
    }

}
