package com.mhtocs;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.commons.io.input.CountingInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SingleMain {
    public static void main(String[] args) throws IOException {
        String connectionString = "DefaultEndpointsProtocol=https;AccountName=l3ctest;AccountKey=wQ7+XNskKY6/SRiMqsd/B4SWLzPW37ABvzr15/5MJsjKCUjsDGF8Nerr+kBSSJmd41i80+Q/89Qv+ASt4K3x8g==;EndpointSuffix=core.windows.net";
        String containerName = "insights-activity-logs";


        // Create a BlobClientBuilder using the connection string and container name
        BlobServiceClient serviceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);

        long totalLogs = 0;
        long totalBytesRead = 0;


        long start = System.currentTimeMillis();


        String blobName = "resourceId=/SUBSCRIPTIONS/2B26B638-387C-422B-8C0D-F09438B2EF0E/y=2024/m=01/d=18/h=08/m=00/PT1H.json";

        BlobClient blobClient = containerClient.getBlobClient(blobName);

        int count = 0;
        long bytesRead = 0;

        // Download to append blob
        try (InputStream is  = blobClient.openInputStream();
             CountingInputStream cis = new CountingInputStream(is);
             BufferedReader reader = new BufferedReader(new InputStreamReader(cis), 8192)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
                count += 1;
                System.out.println(cis.getByteCount());
                break;
            }

            totalBytesRead += cis.getByteCount();
            totalLogs += count;
        }



        long end = System.currentTimeMillis();
        long total = (end - start) / 1000;
        double totalMB = totalBytesRead / (1024.0 * 1024.0);
        System.out.println("Total logs downloaded: " + totalLogs+ " in "+ total+ " seconds");
        System.out.println("Total data read: " + totalMB+ " mb");

    }
}
