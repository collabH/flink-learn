package com.learn.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @fileName: LedgerNewAPI.java
 * @description: ledger new api
 * @author: huangshimin
 * @date: 2023/5/31 19:14
 */
public class LedgerNewAPI {
    public static void main(String[] args) throws BKException, IOException, InterruptedException, ExecutionException {
        ClientConfiguration conf = new ClientConfiguration();
        String metaServiceUrl = "zk+hierarchical://localhost:2181/ledgers";
        conf.setMetadataServiceUri(metaServiceUrl);

        // 创建Bookkeeper客户端
        BookKeeper bk = BookKeeper.newBuilder(conf)
                .build();
        // 创建ledger
        byte[] password = "new-api-ledger".getBytes();

        WriteHandle wh = bk.newCreateLedgerOp()
                .withDigestType(DigestType.CRC32)
                .withPassword(password)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(3)
//                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .execute()
                .get();
        long ledgerId = wh.getId();
        // 创建entry
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.put("hhh".getBytes());
        byteBuffer.position(0);
        long entryId = wh.append(byteBuffer);
        wh.close();
        ReadHandle rh = bk.newOpenLedgerOp().withLedgerId(ledgerId)
                .withPassword(password)
                .withDigestType(DigestType.CRC32)
                .withRecovery(true)
                .execute()
                .get();
        LedgerEntries entries = rh.read(entryId, 99);
        for (LedgerEntry entry : entries) {
            System.out.println(Arrays.toString(entry.getEntryBytes()));
        }
        rh.close();
        bk.close();
    }
}
