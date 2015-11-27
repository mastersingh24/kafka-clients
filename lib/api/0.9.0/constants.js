/**
 * Enum for Kafka APIs
 * @enum {number}
 */
exports.KAFKA_APIS = {
    PRODUCE: 0,
    FETCH: 1,
    LIST_OFFSETS: 2,
    METADATA: 3,
    LEADER_AND_ISR: 4,
    STOP_REPLICA: 5,
    UPDATE_METADATA_KEY: 6,
    CONTROLLED_SHUTDOWN_KEY: 7,
    OFFSET_COMMIT: 8,
    OFFSET_FETCH: 9,
    GROUP_COORDINATOR: 10,
    JOIN_GROUP: 11,
    HEARTBEAT: 12,
    LEAVE_GROUP: 13,
    SYNC_GROUP: 14,
    DESCRIBE_GROUPS: 15,
    LIST_GROUPS: 16    
};

/**
 * Kafka Errors
 */
exports.KAFKA_ERRORS = [
    "NoError",
    "OffsetOutOfRange",
    "InvalidMessage",
    "UnknownTopicOrPartition",
    "InvalidMessageSize",
    "LeaderNotAvailable",
    "NotLeaderForPartition",
    "RequestTimedOut",
    "BrokerNotAvailable",
    "ReplicaNotAvailable",
    "MessageSizeTooLarge",
    "StaleControllerEpochCode",
    "OffsetMetadataTooLargeCode",
    "13",
    "GroupLoadInProgressCode",
    "GroupCoordinatorNotAvailableCode",
    "NotCoordinatorForGroupCode",
    "InvalidTopicCode",
    "RecordListTooLargeCode",
    "NotEnoughReplicasCode",
    "NotEnoughReplicasAfterAppendCode",
    "InvalidRequiredAcksCode",
    "IllegalGenerationCode",
    "InconsistentGroupProtocolCode",
    "InvalidGroupIdCode",
    "UnknownMemberIdCode",
    "InvalidSessionTimeoutCode",
    "RebalanceInProgressCode",
    "InvalidCommitOffsetSizeCode",
    "TopicAuthorizationFailedCode",
    "GroupAuthorizationFailedCode",
    "ClusterAuthorizationFailedCode"
];

exports.MAGIC_BYTE = 0;
exports.MESSAGE_ATTRIBUTES_DEFAULT = 0;

//Fetch API
exports.FETCH_MIN_BYTES_DEFAULT=1; //single byte
exports.MAX_PARTITION_FETCH_BYTES_DEFAULT=1048576; //1MB
exports.FETCH_MAX_WAIT_MS_DEFAULT=500;

//Offset API
exports.OFFSET_LATEST=-1;
exports.OFFSET_EARLIEST=-2;
exports.MAX_NUMBER_OF_OFFSETS=100;