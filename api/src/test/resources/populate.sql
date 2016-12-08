INSERT INTO dataset(name) VALUES ('dataset1');
INSERT INTO dataset(name) VALUES ('dataset2');

INSERT INTO batch(dataset,batchkey,type) VALUES (1, 1000001, 'TOTAL');
INSERT INTO batch(dataset,batchkey,type) VALUES (2, 1000002, 'TOTAL');
INSERT INTO batch(dataset,batchkey,type) VALUES (2, 1000003, 'TOTAL');

INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_1', 't1_1_1', 'data1_1_1', 'chksum1_1_1', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_2', 't1_1_2', 'data1_1_2', 'chksum1_1_2', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_3', 't1_1_3', 'data1_1_3', 'chksum1_1_3', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_4', 't1_1_4', 'data1_1_4', 'chksum1_1_4', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_5', 't1_1_5', 'data1_1_5', 'chksum1_1_5', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_6', 't1_1_6', 'data1_1_6', 'chksum1_1_6', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_7', 't1_1_7', 'data1_1_7', 'chksum1_1_7', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_8', 't1_1_8', 'data1_1_8', 'chksum1_1_8', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_9', 't1_1_9', 'data1_1_9', 'chksum1_1_9', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (1, 1, 123456, 'local1_1_10', 't1_1_10', 'data1_1_10', 'chksum1_1_10', 'ACTIVE');

INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_1', 't2_2_1', 'data2_2_1', 'chksum2_2_1', 'RESET');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_2', 't2_2_2', 'data2_2_2', 'chksum2_2_2', 'DELETED');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_3', 't2_2_3', 'data2_2_3', 'chksum2_2_3', 'RESET');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_4', 't2_2_4', 'data2_2_4', 'chksum2_2_4', 'DELETED');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_5', 't2_2_5', 'data2_2_5', 'chksum2_2_5', 'RESET');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_6', 't2_2_6', 'data2_2_6', 'chksum2_2_6', 'DELETED');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_7', 't2_2_7', 'data2_2_7', 'chksum2_2_7', 'RESET');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_8', 't2_2_8', 'data2_2_8', 'chksum2_2_8', 'DELETED');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_9', 't2_2_9', 'data2_2_9', 'chksum2_2_9', 'RESET');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (2, 2, 123456, 'local2_2_10', 't2_2_10', 'data2_2_10', 'chksum2_2_10', 'DELETED');

INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_1', 't3_2_1', 'data3_2_1', 'chksum3_2_1', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_2', 't3_2_2', 'data3_2_2', 'chksum3_2_2', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_3', 't3_2_3', 'data3_2_3', 'chksum3_2_3', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_4', 't3_2_4', 'data3_2_4', 'chksum3_2_4', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_5', 't3_2_5', 'data3_2_5', 'chksum3_2_5', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_6', 't3_2_6', 'data3_2_6', 'chksum3_2_6', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_7', 't3_2_7', 'data3_2_7', 'chksum3_2_7', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_8', 't3_2_8', 'data3_2_8', 'chksum3_2_8', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_9', 't3_2_9', 'data3_2_9', 'chksum3_2_9', 'ACTIVE');
INSERT INTO record(batch,dataset,agencyid,localid,trackingid,content,checksum,status) VALUES (3, 2, 123456, 'local3_2_10', 't3_2_10', 'data3_2_10', 'chksum3_2_10', 'DELETED');