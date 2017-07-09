package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Jenson on 2017/6/19.
 */
public class FileReadCache {
    private Logger logger = LoggerFactory.getLogger(Server.class);
    private final static int SIZE = (512*1024); //512KB
    private final int FILENO;
    private byte[][] bufs;

    private int[] lens;

    private int pos, readPt, writePt;
    private RandomAccessFile raf;

    private final ReentrantLock lock = new ReentrantLock();

    // write condition
    private final Condition notFull = lock.newCondition();

    // read condition
    private final Condition notEmpty = lock.newCondition();

    public FileReadCache(final int fileNO) {
        FILENO = fileNO;
        bufs = new byte[2][];
        bufs[0] = new byte[SIZE];
        bufs[1] = new byte[SIZE];
        lens = new int[2];
    }

    public boolean start() {
        final String fileName = Paths.get(Constants.DATA_HOME, FILENO + ".txt").toString();
        File file = new File(fileName);
        if(file.exists()) {
            try {
                // 原数据文件
                raf = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException e) {
                logger.info(e.getMessage(), e);
                return false;
            } catch (IOException e) {
                logger.info(e.getMessage(), e);
                return false;
            }
        }else{
            return false;
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                startReadFiles();
            }
        }).start();
        return true;
    }

    public void getBuf() {
        lock.lock();
        try {
            while (readPt == writePt)
                notEmpty.await();
            readPt = (readPt + 1) & 1;
            pos = 0;
            notFull.signal();
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    public byte read() {
        if (pos >= lens[readPt]) {
            getBuf();
        }
        if (lens[readPt] == 0)
            return -1;
        return bufs[readPt][pos++];
    }


    public void startReadFiles() {
        if(raf == null) {
            return;
        }
        while (true) {
            int n = -1;
            lock.lock();
            try {
                while (readPt != writePt)
                    notFull.await();
                writePt = (writePt + 1) & 1;
                n = raf.read(bufs[writePt]);
                if (n == -1)
                    lens[writePt] = 0;
                else
                    lens[writePt] = n;
                notEmpty.signal();
            } catch (InterruptedException e) {
                logger.info(e.getMessage(), e);
            } catch (IOException e) {
                logger.info(e.getMessage(), e);
            } finally {
                lock.unlock();
            }
            if (n == -1) {
                try {
                    raf.close();
                } catch (IOException e) {
                    logger.info(e.getMessage(), e);
                }
                return;
            }
        }
    }
}
