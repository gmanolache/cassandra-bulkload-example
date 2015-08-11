package bulkload;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import javax.validation.constraints.Null;


public class GetObjects
{
    private static String bucketName = "sien-data-pipeline-prod";
    private static String key        = "adunblock/detect/2015/07/31/000002_10_142_252_230_19467.txt.gz";
    private static String route      = "adunblock/detect/2015/";

    public static String[] ListKeys()
    {
        List<String> keyList = new ArrayList<>();
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            System.out.println("Listing objects");

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(route);
            ObjectListing objectListing;
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary :
                        objectListing.getObjectSummaries()) {
                    System.out.println(" - " + objectSummary.getKey());
                    keyList.add(objectSummary.getKey());
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
                    "which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
                    "which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return keyList.toArray(new String[keyList.size()]);
    }

    public static String GetObject(String objectKey) {
        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            System.out.println("Downloading " + objectKey);
            S3Object s3object = s3Client.getObject(new GetObjectRequest(
                    bucketName, objectKey));
            try {
                return displayTextInputStream(s3object.getObjectContent());
            } catch (IOException ignored) {
            }

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
                    " means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means" +
                    " the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return null;
    }

    private static String displayTextInputStream(InputStream input)
            throws IOException {

        Reader reader = null;
        StringWriter writer = null;
        String charset = "UTF-8"; // You should determine it based on response header.

        try {
            InputStream gzipInputStream = new GZIPInputStream(input);
            reader = new InputStreamReader(gzipInputStream, charset);
            writer = new StringWriter();

            char[] buffer = new char[8192];
            for (int length = 0; (length = reader.read(buffer)) > 0; ) {
                writer.write(buffer, 0, length);
            }
        }catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(writer != null)
                writer.close();
            if(writer != null)
                reader.close();
        }

        return writer == null? null : writer.toString();
    }
}