package com.mhtocs;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.commons.io.input.CountingInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class AzureActivityLogsReader {
    public static void main(String[] args) throws IOException {

        // Load environment variables from .env file
        Dotenv dotenv = Dotenv.load();
        String connectionString = dotenv.get("STORAGE_CONNECTION_STRING");;
        String subscriptionID = dotenv.get("SUBSCRIPTION_ID");

        String containerName = "insights-activity-logs";
        String PATH = "resourceId=/SUBSCRIPTIONS/%s/y=2024/m=01/d=18/h=%02d/m=00/PT1H.json";


        // Create a BlobClientBuilder using the connection string and container name
        BlobServiceClient serviceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);

        long totalLogs = 0;
        long totalBytesRead = 0;


        long start = System.currentTimeMillis();

        for (int hour = 1; hour <= 23; hour++) {

            String blobName = String.format(PATH, subscriptionID, hour);
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
                    System.out.println(line);
//                    bytesRead += cis.getByteCount();
                    count += 1;
                }

                totalBytesRead += cis.getByteCount();
                totalLogs += count;
            }

        }

        long end = System.currentTimeMillis();
        long total = (end - start) / 1000;
        double totalMB = totalBytesRead / (1024.0 * 1024.0);
        System.out.println("Total logs downloaded: " + totalLogs+ " in "+ total+ " seconds");
        System.out.println("Total data read: " + totalMB+ " mb");

    }
}