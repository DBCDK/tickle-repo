INSERT INTO dataset(name) VALUES ('dataset1');

INSERT INTO batch(dataset,batchkey,type) VALUES (1, 1000000, 'TOTAL');

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