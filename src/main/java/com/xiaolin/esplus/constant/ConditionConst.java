package com.xiaolin.esplus.constant;

public class ConditionConst {
    /**
     * 等于 =
     */
    public static final String EQ = "eq";
    /**
     * 不等于 <>
     */
    public static final String NE = "ne";
    /**
     * 模糊查询 like '%XX%'
     */
    public static final String LK = "like";
    /**
     * 以什么结尾 like '%XX'
     */
    public static final String LLK = "likeLeft";
    /**
     * 以什么开头 like 'XX%'
     */
    public static final String RLK = "likeRight";
    /**
     * 不包含 not like '%XX%'
     */
    public static final String NC = "notLike";
    /**
     * 不以什么结尾 not like '%XXX'
     */
    public static final String NEL = "notLikeLeft";
    /**
     * 不以什么开头 not like 'XXX%'
     */
    public static final String NBL = "notLikeRight";
    /**
     * 在其中 in('A', 'B')
     */
    public static final String IN = "in";
    /**
     * 不在其中 not in('A', 'B')
     */
    public static final String NIN = "notIn";
    /**
     * 大于 >
     */
    public static final String GT = "gt";
    /**
     * 大于等于 >=
     */
    public static final String GE = "ge";
    /**
     * 小于 <
     */
    public static final String LT = "lt";
    /**
     * 小于等于 <=
     */
    public static final String LE = "le";
    /**
     * 在XXX之间 between 1 and 2
     */
    public static final String BT = "between";
    /**
     * 不在XXX之间 not between 1 and 2
     */
    public static final String NBT = "notBetween";
    /**
     * 为空 is null
     */
    public static final String NU = "isNull";
    /**
     * 不为空 is not null
     */
    public static final String NN = "isNotNull";

    /**
     * 分词匹配
     */
    public static final String MQ = "mq";
    /**
     * 分词不匹配
     */
    public static final String NM = "notMq";
    /**
     * 嵌套匹配
     */
    public static final String NESTED_EQ = "nestedEq";
    /**
     * 嵌套不匹配
     */
    public static final String NESTED_IN = "nestedIn";
}
