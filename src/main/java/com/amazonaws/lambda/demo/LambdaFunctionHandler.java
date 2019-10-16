package com.amazonaws.lambda.demo;

import java.util.List;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.metaopsis.icsapi.helper.*;
import com.metaopsis.icsapi.impl.InformaticaCloudException;
import com.metaopsis.icsapi.impl.InformaticaCloudImpl;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    public LambdaFunctionHandler() {}

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        List<S3EventNotificationRecord>  records = event.getRecords();
        for (S3EventNotificationRecord record : records)
        {
        		String filename = record.getS3().getObject().getKey();
        		context.getLogger().log("FileName : " + filename);
        		if (record.getS3().getObject().getSizeAsLong() > 0)
        		{
        			InformaticaCloudImpl impl = new InformaticaCloudImpl();
        			try {
						User user = impl.login(new Login("tbennett@unicosolution.app.com","Lak3v13w.c0m"));
						String jobname = "mct_aws_s3_" + UUID.randomUUID().toString();
						String mct = "{\"@type\": \"mtTask\",\"orgId\": \"0018IZ\",\"name\": \""+ jobname + "\",\"description\": \"\",\"runtimeEnvironmentId\": \"0018IZ2500000000001X\",\"mappingId\": \"0018IZ170000000000GL\",\"parameters\": [{\"@type\": \"mtTaskParameter\",\"name\": \"$NewSource$\",\"type\": \"EXTENDED_SOURCE\",\"label\": \"NewSource\",\"sourceConnectionId\": \"0018IZ0B0000000000G1\",\"extendedObject\": {\"@type\": \"extendedObject\",\"object\": {\"@type\": \"mObject\",\"name\": \""+ filename + "\",\"label\": \"" + filename + "\"}}}]}";
						impl.createMappingConfigurationTask(user, mct);
						Job job = new Job();
						job.setTaskName(jobname);
						job.setTaskType("MTT");
						job.setCallbackURL("https://3gz3b7fhek.execute-api.us-east-1.amazonaws.com/alpha");
						Job response = impl.job(user, job, true);
						
						context.getLogger().log(response.toString());
        			
        			} catch (InformaticaCloudException e) {
						// TODO Auto-generated catch block
						context.getLogger().log(e.getMessage());
					}
        		}
        		
        }
        
        
        return null;
        // Get the object from the event and show its content type
        /*String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        try {
            S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
            String contentType = response.getObjectMetadata().getContentType();
            context.getLogger().log("CONTENT TYPE: " + contentType);
            return contentType;
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", key, bucket));
            throw e;
        } */
    }
}