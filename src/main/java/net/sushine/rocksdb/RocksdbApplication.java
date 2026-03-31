package net.sushine.rocksdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.rocksdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootApplication
public class RocksdbApplication {

    private static final String DB_PATH_STR = "my_rocksdb_data";

    public static void main(String[] args) {
        SpringApplication.run(RocksdbApplication.class, args);

        if (args.length < 1) {
            System.out.println("Cannot find source data file,please check!");
            System.exit(1);
        }

        String dataFilePathStr = args[0];
        Path dbPath = Paths.get(DB_PATH_STR);
        Path dataFilePath = Paths.get(dataFilePathStr);

        // --- 1. 检查并创建数据库目录 ---
        try {
            if (!Files.exists(dbPath)) {
                System.out.println("Creating DB directory: " + dbPath.toAbsolutePath());
                Files.createDirectories(dbPath);
            } else if (!Files.isDirectory(dbPath)) {
                System.err.println("Error: DB path exists but is not a directory!");
                return;
            }
        } catch (IOException e) {
            System.err.println("Failed to create DB directory: " + e.getMessage());
            return;
        }

        // --- 2. 检查数据文件是否存在 ---
        if (!Files.exists(dataFilePath)) {
            System.err.println("Error: Data file not found at: " + dataFilePath.toAbsolutePath());
            System.err.println("Please create a 'data.txt' file with format 'key:value'");
            return;
        }

        System.out.println(">>> RocksDB Database Absolute Path: " + dbPath.toAbsolutePath());
        System.out.println(">>> Reading data from: " + dataFilePath.toAbsolutePath());

        // --- 3. 打开 RocksDB 并写入数据 ---
        int successCount = 0;
        int failCount = 0;

        // 使用 try-with-resources 自动关闭 RocksDB 和 文件流
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, DB_PATH_STR);
             BufferedReader reader = new BufferedReader(new FileReader(dataFilePath.toFile()))) {

            String line;
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();

                // 跳过空行或注释行
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // 解析 key:value (只分割第一个冒号，防止 value 中包含冒号)
                int separatorIndex = line.indexOf(':');
                if (separatorIndex == -1) {
                    System.err.println("Line " + lineNum + " skipped (invalid format): " + line);
                    failCount++;
                    continue;
                }

                String key = line.substring(0, separatorIndex);
                String value = line.substring(separatorIndex + 1);

                if (key.isEmpty()) {
                    System.err.println("Line " + lineNum + " skipped (empty key): " + line);
                    failCount++;
                    continue;
                }

                try {
                    // 写入 RocksDB
                    db.put(key.getBytes(), value.getBytes());
                    successCount++;
                    // 可选：打印成功日志，数据量大时建议注释掉以提高性能
                    // System.out.println("Inserted: " + key);
                } catch (RocksDBException e) {
                    System.err.println("Line " + lineNum + " failed to write: " + e.getMessage());
                    failCount++;
                }
            }

            System.out.println("--------------------------------");
            System.out.println("Import Finished!");
            System.out.println("Total lines processed: " + lineNum);
            System.out.println("Successfully written: " + successCount);
            System.out.println("Failed/Skipped: " + failCount);
            System.out.println("--------------------------------");
        } catch (IOException e) {
            System.err.println("File IO Error: " + e.getMessage());
            e.printStackTrace();
        } catch (RocksDBException e) {
            System.err.println("RocksDB Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
