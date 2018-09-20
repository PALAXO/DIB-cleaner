import helpers
import time
import logging.handlers

# helpers init
conf = helpers.ConfigOperations()
fSystem = helpers.OsOperations()

# logger init
logger = logging.getLogger('webScanCleaner')
loggerFormatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# file logging
fileHandler = logging.handlers.RotatingFileHandler(conf.filePath, maxBytes=10485760, backupCount=3000, encoding='utf-8')
fileHandler.setFormatter(loggerFormatter)
logger.addHandler(fileHandler)

# console logging
# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(loggerFormatter)
# logger.addHandler(consoleHandler)

logger.setLevel(logging.DEBUG)

# query files loading
query_file = open("query.ini", "r")
query = query_file.read()
query_file.close()

revisions_query_file = open("revisions-query.ini", "r")
revisions_query = revisions_query_file.read()
revisions_query_file.close()


def delete_all_old():
    """
    deletes all old records, regardless of revisions
    """
    try:
        result = es.get_data(conf.index, query)
        records = result["hits"]["hits"]
    except Exception as ex:
        logger.error(ex)
        logger.error("Unable to fetch records from Es server. Going to retry after 30 seconds")
        time.sleep(30)
        return
    logger.info("Processing "+str(len(records))+" records returned from ES")
    for record in records:
        time.sleep(1)
        perform_delete(record)


def delete_old_revisions():
    """
    deletes older revisions of existing documents, regardless of their age
    """
    try:
        result = es.get_data(conf.index, revisions_query)
        app_no_buckets = result["aggregations"]["appNo"]["buckets"]
    except Exception as ex:
        logger.error(ex)
        logger.error("Unable to fetch records from Es server. Going to retry after 30 seconds")
        time.sleep(30)
        return
    for app_no_bucket in app_no_buckets:
        doc_id_buckets = app_no_bucket["docId"]["buckets"]
        for doc_id_bucket in doc_id_buckets:
            records = doc_id_bucket["top_documents_hits"]["hits"]["hits"]
            i = 1
            for record in records:
                # do not delete the latest revision
                if i != 1:
                    perform_delete(record)
                i += 1


def perform_delete(record):
    """
    performs the deletion from filesystem and updates the elasticsearch flag
    """
    app_no = None
    doc_id = None
    record_type = None
    record_id = None
    if '_id' in record:
        doc_id = str(record['_id'])
        record_id = str(record['_id'])
    if 'appNo' in record['_source']:
        app_no = str(record['_source']['appNo'])
    if '_type' in record:
        record_type = str(record['_type'])
    logger.info('Processing app_No=' + app_no + ' document_Id=' + doc_id)
    if 'path' in record['_source'] and len(record['_source']['path']) > 3:
        try:
            fSystem.purge_directory(record['_source']['path'])
            logger.info('Successfully purged directory '+record['_source']['path'])
        except Exception as ex:
            logger.warn(ex)
        try:
            response = es.update_record(conf.index, record_type, record_id)
            logger.info("Successfully updated ElasticSearch Record. " + response["_index"]+"/"+response["_type"]+"/"+response["_id"])
        except Exception as ex:
            logger.warn("Unable to update record in ES server")
            logger.warn(ex)
    else:
        logger.warning("Path variable not found")


while True:
    logger.info("Attempting to connect to ElasticSearch server")
    es = helpers.EsOperations()
    while es.connect(conf.host, conf.port) is False:
        logger.warn("Unable to connect to ES instance. Will retry after 30 seconds")
        time.sleep(30)
    logger.info("Successfully connected to ElasticSearch server")
    logger.info("======Starting cronRun V3.0======")

    delete_all_old()
    delete_old_revisions()

    logger.info("Going to sleep for " + conf.interval + " seconds")
    time.sleep(float(conf.interval))
