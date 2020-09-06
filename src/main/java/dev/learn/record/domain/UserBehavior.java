package dev.learn.record.domain;

import lombok.Data;

/**
 * @fileName: UserBehavior.java
 * @description: UserBehavior.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 8:57 下午
 */
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis()/1000);
    }
}

