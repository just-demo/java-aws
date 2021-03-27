package demo;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;

import java.util.Map;
import java.util.stream.Collectors;

public class RdsUtils {
    private static final RdsClient CLIENT = RdsClient.builder()
            // .region(region)
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
