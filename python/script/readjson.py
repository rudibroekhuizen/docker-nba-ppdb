import json
import logging
from schema import *
from pony.orm import *
from hashlib import md5
from timeit import default_timer as timer

FORMAT = u'%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('ppd_logger')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(FORMAT)

logfile = '/var/log/app/ppd_logger.log'
fh = logging.FileHandler(logfile, encoding = "UTF-8")

fh.setFormatter(formatter)
fh.setLevel(logging.INFO)
logger.addHandler(fh)

class JsonImporter():
    counter = {}
    line = ""
    filename = ""

    def __init__(self, filename=''):
        self.filename = filename
        self.counter = {
            'lines':0, 
            'new':0, 
            'updated':0, 
            'skipped':0, 
            'start': 0, 
            'end': 0, 
            'elapsed': 0
        }
        self.line = ""

    def mark_time(self):
        if (self.counter['start'] == 0) :
            self.counter['start'] = timer()
        self.counter['end'] = timer()
        self.counter['elapsed'] = (self.counter['end'] - self.counter['start'])
        if not (self.counter['lines'] % 1000) :
            print('.', end='', flush=True)


    def handle_line(self):
        self.counter['lines'] += 1
        rec = json.loads(self.line.strip())
        json_hash = md5(self.line.encode('utf-8').strip()).hexdigest()

        document = Document.get(record_id = rec['unitID'])
        if (document):
            if (json_hash != document.hash) :
                self.updateDocument(document,rec,json_hash)
            else :
                self.counter['skipped'] += 1
                logger.debug("%d - Not updated: %s - %0.2f" % (self.counter['lines'], rec['unitID'], self.counter['elapsed']))
        else :
            self.storeDocument(rec,json_hash)


    def updateDocument(self, document, rec, json_hash):
        scigroup = rec['identifications'][0]['scientificName']['scientificNameGroup']
        document.hash = json_hash
        document.source = rec['sourceSystem']['code']
        document.record = self.line
        document.status = 'Updated'
        document.nds_index = 'specimen'
        document.documenttype = 'specimen'
        document.scientific_name_group = scigroup 

        self.counter['updated'] += 1

        logger.info("%d - Record updated: %s - %0.2f" % (self.counter['lines'], rec['unitID'], self.counter['elapsed']))

    def storeDocument(self, rec, json_hash):
        scigroup = rec['identifications'][0]['scientificName']['scientificNameGroup']
        try:
            document = Document(
                record_id = rec['unitID'],
                hash = json_hash,
                source = rec['sourceSystem']['code'],
                record = self.line,
                status = 'New',
                nds_index = 'specimen',
                documenttype = 'specimen',
                scientific_name_group = scigroup 
            )
            logger.info("Document record stored: %s" % (rec['unitID']))
            self.counter['new'] += 1
        except:
            logger.error("Document already stored: %s" % (rec['unitID']))
            self.counter['skipped'] += 1


    @db_session
    def parse_json(self):
        self.mark_time()
        with open(self.filename, 'r') as f:
            for self.line in f:
                self.handle_line()
                self.mark_time()
        self.report()

    def report(self):
        print('.')
        logger.info("Execution time %0.2f seconds" % (self.counter['elapsed']))
        logger.info("%d lines processed" % (self.counter['lines']))
        logger.info("%d new records" % (self.counter['new']))
        logger.info("%d records updated" % (self.counter['updated']))
        logger.info("%d records skipped" % (self.counter['skipped']))
                
importer = JsonImporter(filename='/data/validation-output-20180626-104522--000.json')
importer.parse_json()
