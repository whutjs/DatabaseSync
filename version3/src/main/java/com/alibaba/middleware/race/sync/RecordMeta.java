package com.alibaba.middleware.race.sync;

/** 用long 64bit表示一条记录
 * Created by Jenson on 2017/6/18.
 */
public class RecordMeta {
    /**
     * 表示方法：记录总共有5列，假设顺序是first_name,last_name,sex,score, score2;
     * 只有score和score2是数字，而且范围是[0,300039]，能用19bit表示得下；
     * sex用1bit表示：0是男，1是女；
     * first_name和last_name根据对最后120万结果的统计，有1923个不同的取值。那么可以用一个ArrayList存这些值，然后
     * 在这里只记录这些值的idx，那么可以用12bit来表示。
     * 所以总共占用bit: 19+19+1+12+12=63bit。
     * 从最右边开始存，分别是：
     * 63|62       51|50     39|38|37  19|18    0
     * +---------------------------------------+
     * |0|first_name|last_name|sex|score|score2|
     * +--------------------------------------+
     */
    public long metaData;

    public RecordMeta() {
        this.metaData = 0;
    }
}
