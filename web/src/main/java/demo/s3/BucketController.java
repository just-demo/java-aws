package demo.s3;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

import java.util.List;

import static java.util.stream.Collectors.toList;

@RestController
@RequestMapping("/buckets")
public class BucketController {
    @GetMapping
    public List<String> list() {
        S3Client client = S3Client.builder().build();
        ListBucketsRequest request = ListBucketsRequest.builder().build();
        ListBucketsResponse response = client.listBuckets(request);
        return response.buckets().stream()
                .map(Bucket::name)
                .collect(toList());
    }
}
