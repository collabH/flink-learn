package com.learn.bookkeeper.client;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;

/**
 * @fileName: LedgerApiClient.java
 * @description: bookkeeper ledger api client
 * @author: huangshimin
 * @date: 2023/5/31 15:15
 */
public class LedgerApiClient {
    public static void main(String[] args) {
        try {
            String zkConnectionString = "localhost:2181";
            String metaServiceUrl = "zk+hierarchical://localhost:2181/ledgers";
            BookKeeper bookKeeper = confBkCls(metaServiceUrl);
            byte[] password = "test-ledger".getBytes(StandardCharsets.UTF_8);
            // 同步创建ledger
            LedgerHandle ledger = bookKeeper.createLedgerAdv(3, 2, 2,
                    BookKeeper.DigestType.CRC32, password);
            long ledgerId = ledger.getId();
            System.out.println(ledgerId);
            // 异步创建ledger
//            bookKeeper.asyncCreateLedger(3,
//                    2,
//                    BookKeeper.DigestType.MAC,
//                    password,
//                    new AsyncCallback.CreateCallback() {
//                        @Override
//                        public void createComplete(int rc, LedgerHandle lh, Object ctx) {
//                            System.out.println("Ledger successfully created");
//                        }
//                    },
//                    "some context");

            // 创建entry至ledger中
            long entryId = ledger.addEntry(1L, "bookkeeper-entry".getBytes(StandardCharsets.UTF_8));
            Enumeration<LedgerEntry> ledgerEntryEnumeration = ledger.readEntries(1, 99);
            while (ledgerEntryEnumeration.hasMoreElements()) {
                System.out.println(Arrays.toString(ledgerEntryEnumeration.nextElement().getEntry()));
            }
            bookKeeper.deleteLedger(5);

        } catch (InterruptedException | IOException | BKException e) {
            e.printStackTrace();
        }
    }

    /**
     * conf方式创建BookKeeper对象
     *
     * @param zkConnectionString
     * @throws BKException
     * @throws IOException
     * @throws InterruptedException
     */
    private static BookKeeper confBkCls(String zkConnectionString) throws BKException, IOException,
            InterruptedException {
        ClientConfiguration config = new ClientConfiguration();
        config.setMetadataServiceUri(zkConnectionString);
        config.setAddEntryTimeout(2000);
//        BookKeeper bkClient = new BookKeeper(config);
        return BookKeeper.forConfig(config).build();
    }

    private static BookKeeper NoConfBkClient(String zkConnectionString) throws IOException, InterruptedException,
            BKException {
        return new BookKeeper(zkConnectionString);
    }
}
