package self.ed.aws;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class S3Utils {
    private static S3Client CLIENT = S3Client.builder()
            // TODO: it does not work for other regions than default, make it cross region https://stackoverflow.com/questions/46769493/how-enable-force-global-bucket-access-in-aws-s3-sdk-java-2-0
            //.region(AWS_GLOBAL)
            .build();

    public static void main(String[] args) {
         System.out.println(listBuckets().size());
         deleteBuckets();
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

        } while(listResponse.isTruncated());
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
