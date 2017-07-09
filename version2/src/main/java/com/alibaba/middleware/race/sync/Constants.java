package com.alibaba.middleware.race.sync;

/**
 * 外部赛示例代码需要的常量 Created by wanshao on 2017/5/25.
 */
class Constants {
    /** 线上测评的值分别为：10,13,20 */
    // 线程数目
    public static final int THREAD_NUM = 10;
    // 分割成的中间文件个数
    public static final int MIDDLE_FILE_NUM = 13;
    public static final int SKIP_POS = 20;

    public static final byte INSERT_TYPE = -1;
    public static final byte UPDATE_TYPE = -2;
    public static final byte DELETE_TYPE = -3;
    // 假设待查询的表最多有两百万条主键
    public static final int TOTAL_PK_NUM = 2000005;



    // ------------ 本地测试可以使用自己的路径--------------//

    // 工作主目录
    // String TESTER_HOME = "/Users/wanshao/work/middlewareTester";
    // 赛题数据
//      public static final String DATA_HOME = "E:\\Contest\\阿里中间件2017\\第二赛季\\tmp\\canal_data";
//    public static final String RESULT_HOME = "E:\\Contest\\阿里中间件2017\\第二赛季\\tmp";
//    public static final String MIDDLE_HOME = "E:\\Contest\\阿里中间件2017\\第二赛季\\tmp\\middle";

//    public static final String DATA_HOME = "E:\\Contest\\阿里中间件2017\\第二赛季\\tmp\\hello";
//    public static final String DATA_HOME = "E:\\Contest\\阿里中间件2017\\第二赛季\\canal_data";

//    public static final String DATA_HOME = "/home/sfc/jenson/alibaba/2017/race2/data/canal_data";
//   public static final String RESULT_HOME = "/home/sfc/jenson/alibaba/2017/race2";
//    public static final String MIDDLE_HOME = "/home/sfc/jenson/alibaba/2017/race2/middle";

    // teamCode
    public static final String TEAMCODE = "725137k7ga";
    // 日志级别
    public static final String LOG_LEVEL = "INFO";
    // 中间结果目录
//    public static final String MIDDLE_HOME = "/Users/wanshao/work/middlewareTester/middle";
    // server端口
    public static final Integer SERVER_PORT = 5527;

    // ------------ 正式比赛指定的路径--------------//
    //// 工作主目录
    public static final String TESTER_HOME = "/home/admin/logs";

    //// 赛题数据
    public static final String DATA_HOME = "/home/admin/canal_data";
    // 结果文件目录(client端会用到)
    public static final String RESULT_HOME = "/home/admin/sync_results/725137k7ga";

    //// 中间结果目录（client和server都会用到）
    public static final String MIDDLE_HOME = "/home/admin/middle/725137k7ga";

    // 结果文件的命名
    public static final String RESULT_FILE_NAME = "Result.rs";

}