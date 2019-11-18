package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.talend.databricks.dbfs.DBFS;
import com.talend.databricks.dbfs.DBFSException;
import com.talend.databricks.dbfs.Response;


public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    private DBFS dbfs = null;
    public LambdaFunctionHandler() {}

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        if (dbfs == null)
            this.dbfs = DBFS.getInstance(System.getenv("DB_REGION"), System.getenv("DB_TOKEN"));
        String path = System.getenv("DB_PATH");
        context.getLogger().log("Received event: " + event);
        List<S3EventNotificationRecord>  records = event.getRecords();
        try {
            dbfs.mkdirs(path);
        } catch (DBFSException e) {
            context.getLogger().log(e.getMessage());
        }
        ArrayList<String> paths = new ArrayList<String>();
        for (S3EventNotificationRecord record : records)
        {
            S3Object fullObject = null;
            String filename = record.getS3().getObject().getKey();
            AmazonS3 client = new AmazonS3Client();
            S3Object object = client.getObject(new GetObjectRequest(record.getS3().getBucket().getName(),filename));

            context.getLogger().log("FileName : " + filename);
            String xpath = path+"/"+filename;

            try {
                if (!paths.contains(xpath)) {
                    context.getLogger().log("Creating Path " + xpath);
                    int handle = dbfs.create(path + "/" + filename);
                    context.getLogger().log("Handle: " + handle);
                    processFile(object.getObjectContent(), context, handle);
                    dbfs.close(handle);
                    context.getLogger().log("Closing Handle: " + handle);
                } else {
                    context.getLogger().log(xpath + " already exists!");
                }
            } catch(DBFSException e)
            {
                context.getLogger().log(e.getMessage());
            }
        }
        
        
        return null;
    }


    private void processFile(InputStream input, Context context, int handle) {
        // Read the text input stream one line at a time and display each line.
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            String line = null;
            //int chunkSize = 1048576; // 1MB but coding below will not break multibyte characters
            StringBuilder buff = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                buff.append(line + "\n");
            }

            dbfs.addBlock(handle, buff.toString().getBytes("UTF-8"));
        } catch(DBFSException | IOException e)
        {
            context.getLogger().log(e.getLocalizedMessage());
        }
    }
}