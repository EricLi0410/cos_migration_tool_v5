package com.qcloud.cos_migrate_tool.task;

import java.util.concurrent.Semaphore;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.Copy;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCopyBucketRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;

public class MigrateCopyBucketTask extends Task {
    private final COSClient srcCOSClient;
    private final String destRegion;
    private final String destBucketName;
    private final String destKey;
    private final String srcRegion;
    private final String srcBucketName;
    private final String srcKey;
    private final long srcSize;
    private final String srcEtag;

    public MigrateCopyBucketTask(Semaphore semaphore, CopyBucketConfig config,
            TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
            COSClient srcCOSClient, String srcKey, long srcSize, String srcEtag, String destKey) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.srcCOSClient = srcCOSClient;
        this.destRegion = config.getRegion();
        this.destBucketName = config.getBucketName();
        this.destKey = destKey;
        this.srcRegion = config.getSrcRegion();
        this.srcBucketName = config.getSrcBucket();
        this.srcKey = srcKey;
        this.srcSize = srcSize;
        this.srcEtag = srcEtag;
    }



    @Override
    public void doTask() {
        MigrateCopyBucketRecordElement migrateCopyBucketRecordElement =
                new MigrateCopyBucketRecordElement(destRegion, destBucketName, destKey, srcRegion,
                        srcBucketName, srcKey, srcSize, srcEtag);
        if (isExist(migrateCopyBucketRecordElement)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(new Region(srcRegion),
                srcBucketName, srcKey, destBucketName, destKey);
        try {
            Copy copy = smallFileTransfer.copy(copyObjectRequest, srcCOSClient, null);
            copy.waitForCompletion();
            saveRecord(migrateCopyBucketRecordElement);
            TaskStatics.instance.addSuccessCnt();
            String printMsg =
                    String.format("[ok] task_info: %s", migrateCopyBucketRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] task_info: %s",
                    migrateCopyBucketRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    migrateCopyBucketRecordElement.buildKey(),
                    migrateCopyBucketRecordElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }


}
