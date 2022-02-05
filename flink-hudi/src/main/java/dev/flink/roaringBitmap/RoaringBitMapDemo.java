package dev.flink.roaringBitmap;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * @fileName: RoaringBitMapDemo.java
 * @description: 稀疏位图案例
 * @author: huangshimin
 * @date: 2022/1/28 3:12 PM
 */
public class RoaringBitMapDemo {
    public static void main(String[] args) {
        basic();
    }

    /**
     * 基础案例
     */
    public static void basic() {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.add(1, 2, 3, 4);
        bitmap.and(roaringBitmap);
        for (Integer integer : bitmap) {
            System.out.println(integer);
        }
    }

    public static void longBitMap() {
        LongBitmapDataProvider r = Roaring64NavigableMap.bitmapOf(1, 2, 100, 1000);
        r.addLong(1234);
        System.out.println(r.contains(1)); // true
        System.out.println(r.contains(3)); // false
        LongIterator i = r.getLongIterator();
        while (i.hasNext()) System.out.println(i.next());
    }
}
