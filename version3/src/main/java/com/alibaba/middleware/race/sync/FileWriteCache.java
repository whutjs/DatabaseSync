package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Jenson
 *
 */
public final class FileWriteCache {
    Logger logger = LoggerFactory.getLogger(Server.class);

    private final int fileNO;
    private final String filePath;
    // 表示cache已经写到哪个位置了（其实就是长度）
    private int pos;

	private RandomAccessFile raf;
	private FileChannel outChannel;


    // 已经写入文件的字节数
    private long bytesWrite = 0;
    // 256KB
    private final int CACHE_SIZE = (256*1024);

    private ByteBuffer[] caches;
    // 表示当前用于给Producer写入的Cache
    private ByteBuffer curCache;
    private final Lock lock;
    // 表示是否有待写入文件的cache
    private final Condition notEmpty;
    // 表示是否有空闲的cache供producer写入
    private final Condition notFull;
    // 表示作为消费者的FileWriteCache的线程当前读的cache的位置
    private int readPtr;
    // 表示生产者线程当前生产的位置
    private int writePtr;
    // 当readPtr == writePtr时，说明是Empty,当前没有数据供FileWriteCache消费写入文件;
    // 当readPtr != writePtr时，说明是Full，需要等待notFull

    public FileWriteCache(final int no) {
        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.notFull = this.lock.newCondition();
        this.readPtr = this.writePtr = 0;
        this.caches = new ByteBuffer[2];
        this.caches[0] = ByteBuffer.allocate(CACHE_SIZE);
        this.caches[1] = ByteBuffer.allocate(CACHE_SIZE);
        this.curCache =this.caches[this.writePtr];

        this.fileNO = no;
        this.filePath = Paths.get(Constants.MIDDLE_HOME, fileNO + ".txt").toString();;
        this.pos = 0;
        File file = new File(filePath);
        if(file.exists()) {
            file.delete();
        }
        try {
            file.createNewFile();
            this.raf = new RandomAccessFile(file, "rw");
            this.outChannel = this.raf.getChannel();
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }

//        /** 5/31 Update:用FileOutputStream */
//        this.outStream = new FileOutputStream(file, true);

        // 5/30 Update:使用两条线程
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    start();
                } catch (InterruptedException e) {
                    logger.info(e.getMessage(), e);
                }
            }
        }).start();;
    }
    // 在FileWriteCache线程中运行的函数
    private void start() throws InterruptedException {
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
                /** 5/31 更新：用FileOutputStream */
                if(cache.hasRemaining()) {
                    try {
                        this.outChannel.write(cache);
                        this.bytesWrite += cache.remaining();
                        this.pos += cache.remaining();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch(Exception e) {
                        e.printStackTrace();
                    }

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
            }finally {
                this.lock.unlock();
            }
        }
        try {
			this.outChannel.close();
			this.raf.close();
		} catch (IOException e) {
			logger.info(e.getMessage(), e);
		}
    }


    public void write(final byte modifyType, final int pkBefore, final int pkAfter,
                      final byte[] data, final int offset, final int len) {
        // 最多可能有totalBytes: 1表示变更类型，4,4表示是主键；2byte(short)用来表示数据长度
       final int totalBytes = 1 + 4 + 4 + 2 + len;
       if(totalBytes > this.curCache.remaining()) {
           // 如果当前cache满了，切换
           // 该方法属于生产者，需要notFull条件满足
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
       // 第一字节用来表示变更类型
        this.curCache.put(modifyType);
       if(modifyType == Constants.INSERT_TYPE) {
           // 插入变更只有变更后的主键
           this.curCache.putInt(pkAfter);
           // TODO:用2byte表示数据长度；后面统计如果长度不超过byte，用byte
           this.curCache.putShort((short)len);
           this.curCache.put(data, offset, len);
       }else if(modifyType == Constants.DELETE_TYPE) {
           // 只有变更前主键
           this.curCache.putInt(pkBefore);
       }else{
           this.curCache.putInt(pkBefore);
           this.curCache.putInt(pkAfter);
           // TODO:用2byte表示数据长度；后面统计如果长度不超过byte，用byte
           this.curCache.putShort((short)len);
           this.curCache.put(data, offset, len);
       }

    }


    /**
     * 强制flush数据到硬盘。如果cache还有数据，先把数据写进文件。
     */
    public void flush() {
        if(this.curCache != null && this.curCache.position() > 0) {
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

    public void close() {
        this.writePtr = -1;
//		try {
//			this.outChannel.close();
//			this.raf.close();
//		} catch (IOException e) {
//			logger.info(e.getMessage(), e);
//		}
    }
}