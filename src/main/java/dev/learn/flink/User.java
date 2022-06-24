package dev.learn.flink;

/**
 * @fileName: User.java
 * @description: User.java类说明
 * @author: by echo huang
 * @date: 2021/1/22 10:17 下午
 */
public class User {
    private Integer id;
    private String name;

    public static void main(String[] args) {
        System.out.println(20 - (20 + 10) % 10);
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

//    public static void main(String[] args) {
//        BloomFilter bloomFilter = new BloomFilter(2000,8);
//        bloomFilter.setBitsLocation(MemorySegmentFactory.allocateUnpooledOffHeapMemory(1000),10);
//        bloomFilter.addHash(123);
//        bloomFilter.addHash(1234);
//        bloomFilter.addHash(1235);
//        System.out.println(bloomFilter.testHash(123));
//    }

    public void setName(String name) {
        this.name = name;
    }
}
