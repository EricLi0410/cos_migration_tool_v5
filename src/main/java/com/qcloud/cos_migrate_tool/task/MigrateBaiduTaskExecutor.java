package com.qcloud.cos_migrate_tool.task;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qcloud.cos_migrate_tool.config.CopyFromAwsConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class MigrateBaiduTaskExecutor extends TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(MigrateBaiduTaskExecutor.class);


    private String bucketName;
    private String cosFolder;

    private AmazonS3 s3Client;
    private String srcBucket;
    private String srcPrefix;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    private String srcEndpoint;

    private CopyFromAwsConfig config;

    public MigrateBaiduTaskExecutor(CopyFromAwsConfig config) {
        super(MigrateType.MIGRATE_FROM_BAIDU, config);
        this.bucketName = config.getBucketName();
        this.cosFolder = config.getCosPath();

        this.srcAccessKeyId = config.getSrcAccessKeyId();
        this.srcAccessKeySecret = config.getSrcAccessKeySecret();
        this.srcBucket = config.getSrcBucket();
        this.srcEndpoint = config.getSrcEndpoint();
        this.srcPrefix = config.getSrcPrefix();
        this.config = config;

        com.amazonaws.ClientConfiguration awsConf = new com.amazonaws.ClientConfiguration();
        awsConf.setConnectionTimeout(5000);
        awsConf.setMaxErrorRetry(5);
        awsConf.setSocketTimeout(10000);
        awsConf.setMaxConnections(1024);
        // Baidu Cloud support HTTP only
        awsConf.setProtocol(Protocol.HTTP);

        if (!config.getSrcProxyHost().isEmpty()) {
            awsConf.setProxyHost(config.getSrcProxyHost());
        }

        if (config.getSrcProxyPort() > 0) {
            awsConf.setProxyPort(config.getSrcProxyPort());
        }

        AWSCredentials credentials = new BasicAWSCredentials(srcAccessKeyId, srcAccessKeySecret);
        this.s3Client = AmazonS3ClientBuilder.standard().withClientConfiguration(awsConf)
        			.withChunkedEncodingDisabled(true) // neccessary for baidu
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(srcEndpoint, null)).build();
    }

    @Override
    public String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [srcBucketName: %s] [cosFolder: %s], [srcEndPoint: %s], [srcPrefix: %s]",
                SystemUtils.getCurrentDateTime(), config.getSrcBucket(), cosFolder,
                config.getSrcEndpoint(), config.getSrcPrefix());
        return comment;
    }

    @Override
    public String buildTaskDbFolderPath() {
        String temp = String.format("[srcPrefix: %s], [cosFolder: %s]", srcPrefix, cosFolder);
        String dbFolderPath =
                String.format("db/migrate_from_baidu/%s/%s", bucketName, DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    public void buildTask() {

        try {
        		// Baidu cloud do not support list object version 2
        		ListObjectsRequest request = new ListObjectsRequest()
        		    .withBucketName(srcBucket)
        		    .withPrefix(srcPrefix);
        		ObjectListing result;
            do {
                result = s3Client.listObjects(request);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    // AddTask
                    MigrateBaiduTask task = new MigrateBaiduTask(config, s3Client,
                            objectSummary.getKey(), objectSummary.getSize(),
                            objectSummary.getETag(), smallFileTransferManager,
                            bigFileTransferManager, recordDb, semaphore);

                    try {
                        AddTask(task);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }

                }
                request.setMarker(result.getNextMarker());
            } while (result.isTruncated());
        } catch (AmazonServiceException ase) {
            log.error("list fail AmazonServiceException errorcode: {}, msg: {}", ase.getErrorCode(),
                    ase.getMessage());
        } catch (AmazonClientException ace) {
            log.error("list fail AmazonClientException msg: {}", ace.getMessage().toString());
        }

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        this.s3Client.shutdown();
    }
}
