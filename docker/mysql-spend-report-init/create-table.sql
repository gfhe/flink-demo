CREATE TABLE wx_group_report
(
    wx_group_id VARCHAR(255) NOT NULL,
    sender_id   VARCHAR(255) NOT NULL,
    send_time   TIMESTAMP(3) NOT NULL,
    send_count  BIGINT       NOT NULL
);
