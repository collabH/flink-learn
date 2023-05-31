package com.learn.bookkeeper.client;


import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @fileName: AdvancedLedgerAPI.java
 * @description: Advanced Ledger API Demo
 * @author: huangshimin
 * @date: 2023/5/31 19:40
 */
public class AdvancedLedgerAPI {
    public static void main(String[] args) throws BKException, IOException, InterruptedException, ExecutionException {
        ClientConfiguration conf = new ClientConfiguration();
        String metaServiceUrl = "zk+hierarchical://localhost:2181/ledgers";
        conf.setMetadataServiceUri(metaServiceUrl);
        BookKeeper bk = new BookKeeper(conf);
        // 创建Bookkeeper客户端
        byte[] password = "password".getBytes();
        // 指定ledgerId，如果存在则报错 Ledger existed
//        long ledgerId = 1L;
//        LedgerHandle ledgerAdv = bk.createLedgerAdv(ledgerId
//                , 3, 3, 3, BookKeeper.DigestType.CRC32, password,
//                Maps.newHashMap());
        long ledgerId = 1235L;
        bk.newDeleteLedgerOp().withLedgerId(ledgerId)
                .execute().get();
        WriteAdvHandle wah = bk.newCreateLedgerOp()
                .withDigestType(DigestType.MAC)
                .withPassword(password)
                .withEnsembleSize(3)
                .withAckQuorumSize(2)
                .withWriteQuorumSize(3)
                .makeAdv()
                .withLedgerId(ledgerId)
                .execute().get();
        int nums = 100;
        for (int i = 0; i < 100; i++) {
            wah.write(i, ("test" + i).getBytes());
        }
        wah.close();
        ReadHandle rh = bk.newOpenLedgerOp()
                .withDigestType(DigestType.MAC)
                .withLedgerId(ledgerId)
                .withPassword(password)
                .execute().get();
        LedgerEntries entries = rh.read(1L, nums - 1);
        for (LedgerEntry entry : entries) {
            System.out.println(entry.getLedgerId() + ":" + entry.getEntryId());
            System.out.println(new String(entry.getEntryBytes()));
        }
        rh.close();
        bk.close();
    }
}
