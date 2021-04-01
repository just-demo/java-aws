package demo;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;

import java.util.Map;
import java.util.stream.Collectors;

import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class RdsUtils {
    private static final RdsClient CLIENT = RdsClient.builder()
            .region(US_EAST_1)
            .build();

    public static void main(String[] args) {
        System.out.println(listInstances());
    }

    private static Map<String, String> listInstances() {
        DescribeDbInstancesResponse response = CLIENT.describeDBInstances();
        return response.dbInstances().stream()
                .collect(Collectors.toMap(DBInstance::dbInstanceIdentifier, i -> i.endpoint().address()));
    }
}
