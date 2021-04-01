package demo;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.presigner.DefaultS3Presigner;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class S3Utils {
    private static final S3Client CLIENT = S3Client.builder()
            // TODO: it does not work for other regions than default, make it cross region https://stackoverflow.com/questions/46769493/how-enable-force-global-bucket-access-in-aws-s3-sdk-java-2-0
            //.region(AWS_GLOBAL)
            .region(US_EAST_1)
            .build();

    public static void main(String[] args) {
        String timestamp = timestamp();
        String bucket = "java-generated-bucket-" + timestamp;
        String key = "java-generated-key-" + timestamp;
        String content = "java-generated-content-" + timestamp;
        createBucket(bucket);
        createObject(bucket, key, content);
        System.out.println(listBuckets());
        System.out.println(generatePresignedUrl(bucket, key));
        deleteBuckets();
    }

    // https://docs.aws.amazon.com/AmazonS3/latest/dev-retired/ShareObjectPreSignedURLJavaSDK.html
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3-presign.html
    public static String generatePresignedUrl(String bucket, String key) {
        S3Presigner presigner = DefaultS3Presigner.builder()
                // Default region (from aws configure) is used, if the target bucket in a different
                // region then the generated URL will return an error about region mismatch.
                // Moreover, it would even generate URL for non-existent bucket name and object key.
                // .region(US_EAST_1)
                .build();
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(1))
                .getObjectRequest(objectRequest)
                .build();
        return presigner
                .presignGetObject(presignRequest)
                .url().toString();
    }

    public static List<String> listBuckets() {
        ListBucketsRequest request = ListBucketsRequest.builder().build();
        ListBucketsResponse response = CLIENT.listBuckets(request);
        return response.buckets().stream()
                .map(Bucket::name)
                .collect(toList());
    }

    public static void deleteBuckets() {
        listBuckets().forEach(bucket -> {
            try {
                deleteBucket(bucket);
            } catch (Exception e) {
                System.err.println("Error deleting bucket [" + bucket + "]: " + e.getMessage());
            }
        });
    }

    public static void createBucket(String bucket) {
        System.out.println("Creating bucket [" + bucket + "]...");
        try {
            CreateBucketRequest request = CreateBucketRequest.builder()
                    .bucket(bucket)
                    .build();
            // This will be created in default region (aws configure) is not specified in client
            CLIENT.createBucket(request);
        } catch (Exception e) {
            System.err.println("Error creating bucket [" + bucket + "]: " + e.getMessage());
        }
    }

    public static void createObject(String bucket, String key, String content) {
        System.out.println("Creating object [" + bucket + "/" + key + "]...");
        try {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            CLIENT.putObject(objectRequest, RequestBody.fromString(content));
        } catch (Exception e) {
            System.err.println("Error creating object [" + bucket + "/" + key + "]: " + e.getMessage());
        }
    }

    private static void deleteBucket(String bucket) {
        System.out.println("Deleting bucket [" + bucket + "]...");
        deleteObjects(bucket);
        deleteVersions(bucket);
        CLIENT.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
    }

    private static void deleteObjects(String bucket) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response listResponse;

        do {
            listResponse = CLIENT.listObjectsV2(listRequest);
            listResponse.contents().stream().map(S3Object::key).forEach(object -> {
                System.out.println("Deleting object [" + object + "]...");
                CLIENT.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(object).build());
            });
            listRequest = ListObjectsV2Request.builder().bucket(bucket)
                    .continuationToken(listResponse.nextContinuationToken())
                    .build();

        } while (listResponse.isTruncated());
    }

    private static void deleteVersions(String bucket) {
        ListObjectVersionsRequest listRequest = ListObjectVersionsRequest.builder()
                .bucket(bucket)
                .build();
        ListObjectVersionsResponse listResponse = CLIENT.listObjectVersions(listRequest);

        listResponse.versions().forEach(object -> {
            System.out.println("Deleting object version [" + object.key() + ":" + object.versionId() + "]...");
            CLIENT.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(object.key())
                    .versionId(object.versionId())
                    .build());
        });

        listResponse.deleteMarkers().forEach(object -> {
            System.out.println("Deleting object delete markers [" + object.key() + "]...");
            CLIENT.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(object.key())
                    .versionId(object.versionId())
                    .build());
        });
    }

    private static String timestamp() {
        return new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date());
    }
}
