package demo;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.presigner.DefaultS3Presigner;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.time.Duration;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class S3Utils {
    private static S3Client CLIENT = S3Client.builder()
            // TODO: it does not work for other regions than default, make it cross region https://stackoverflow.com/questions/46769493/how-enable-force-global-bucket-access-in-aws-s3-sdk-java-2-0
            //.region(AWS_GLOBAL)
            .build();

    public static void main(String[] args) {
        System.out.println(listBuckets());
        // System.out.println(generatePresignedUrl("bucket-name", "file-name"));
        // deleteBuckets();
    }

    // https://docs.aws.amazon.com/AmazonS3/latest/dev-retired/ShareObjectPreSignedURLJavaSDK.html
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3-presign.html
    public static String generatePresignedUrl(String bucket, String key) {
        S3Presigner presigner = DefaultS3Presigner.builder()
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
}
