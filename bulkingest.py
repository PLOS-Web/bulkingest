import rhyno
import argparse
import subprocess
import os.path
import sys
import logging

# Logging junk
logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('bulkingest.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

# get local files
parser = argparse.ArgumentParser(description='push and ingest a bunch of local files')
parser.add_argument('-p', help="Ingest on production (default is stage)")
parser.add_argument('files', nargs='*', help="files to ingest")
args = parser.parse_args()

logger.info("Handling %s file(s)..." % len(args.files))

ingestion_queue = '/var/spool/ambra/ingestion-queue/'
if args.p:
    ambra_file_host = 'production@rwc-prod-adminambra01.int.plos.org'
    rhino_host = 'http://api.plosjournals.org/v1'
else:
    ambra_file_host = 'stage.plos.org'
    rhino_host = 'http://stage.plosjournals.org/api/v1'

r = rhyno.Rhyno(host=rhino_host)

cantingest = []
ingested = []

for f in args.files:
    logger.info("handling %s ..." % f)
    filename_base = os.path.splitext(os.path.split(f)[1])[0] # is hopefully doi
    try:
        scp = subprocess.Popen(["scp", f, "%s:%s" % (ambra_file_host, ingestion_queue)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        scp.wait()
        logger.info("tranferred.")
        r.ingest(os.path.split(f)[1], force_reingest=True)
        logger.info("ingested.")
        r.publish('10.1371/journal.'+ filename_base)
        logger.info("published.")
        ingested.append(f)

    except Exception, e:
        cantingest.append(f)
        logger.error("Unable to ingest %s ..." % f)
        logger.error(e)

logger.info("FINISHED")
logger.info("Ingested and published %s file(s)" % len(ingested))
logger.info("Failed to ingest %s file(s):" % len(cantingest))
for f in cantingest:
    logger.info("  " + f)

