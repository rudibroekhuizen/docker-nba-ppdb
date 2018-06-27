import json
import logging
from schema import *
from pony.orm import *
from hashlib import md5
from timeit import default_timer as timer

FORMAT = u'%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('ppd_logger')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(FORMAT)

logfile = '/var/log/app/ppd_logger.log'
fh = logging.FileHandler(logfile, encoding = "UTF-8")

fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


@db_session
def parse_json(filename):
    counter = {'lines':0, 'new':0, 'updated':0, 'skipped':0}
    start = timer()
    with open(filename, 'r') as f:
        for line in f:
            counter['lines'] += 1
            rec = json.loads(line.strip())
            json_hash = md5(line.encode('utf-8').strip()).hexdigest()
            scigroup = rec['identifications'][0]['scientificName']['scientificNameGroup']
            d = Document.get(record_id = rec['unitID'])
            if (d):
                if (json_hash != d.hash) :
                    d.hash = json_hash
                    d.source = rec['sourceSystem']['code']
                    d.record = line
                    d.status = 'Updated'
                    d.nds_index = 'specimen'
                    d.documenttype = 'specimen'
                    d.scientific_name_group = scigroup 
                    logger.info("Record updated: %s" % (rec['unitID']))
                    counter['updated'] += 1
                else :
                    logger.debug("Record not updated: %s" % (rec['unitID']))
                    counter['skipped'] += 1
            else :
                try:
                    d = Document(
                            record_id = rec['unitID'],
                            hash = json_hash,
                            source = rec['sourceSystem']['code'],
                            record = line,
                            status = 'New',
                            nds_index = 'specimen',
                            documenttype = 'specimen',
                            scientific_name_group = scigroup 
                    )
                    logger.info("New record stored: %s" % (rec['unitID']))
                    counter['new'] += 1
                except:
                    logger.error("Record already stored: %s" % (rec['unitID']))
                    counter['skipped'] += 1
    end = timer()

    logger.info("Execution time %0.2f seconds" % (end-start))
    logger.info("%d lines processed" % (counter['lines']))
    logger.info("%d new records" % (counter['new']))
    logger.info("%d records updated" % (counter['updated']))
    logger.info("%d records skipped" % (counter['skipped']))
                
parse_json('/data/validation-output-20180626-104522--000.json');
