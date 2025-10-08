from dotenv import load_dotenv
from statistics import mean
import psycopg2
import hashlib
import logging
import datetime
import time
import os

# SYSTEM PARAMETERS
DATA_PATH = "data/original"
COMPRESSED_DATA_PATH = "data/compressed/files"
DECOMPRESSED_DATA_PATH = "data/decompressed"
SEGMENTS_PATH = "data/compressed/segments"

SEGMENT_SIZE = 20           # in bytes
SEGMENTS_FILE_SIZE = 1000   # in segments
ID_SIZE = 3                 # in bytes
HASH_FUNC = "md5"           # none || md5 || sha1 || sha256 || sha512


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger()

load_dotenv()

class DB:
    def __init__(self):
        self.db = psycopg2.connect(
            database = os.getenv("DB_NAME"),
            host = os.getenv("DB_HOST"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
            port = os.getenv("DB_PORT")
        )
        logger.info("Connected to the database")
        self.cursor = self.db.cursor()
    
    def execute(self, callback):
        try:
            callback()
            self.db.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            exit(-1)

    def create_hash_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS hash_table(
            id SERIAL PRIMARY KEY,
            hash TEXT,
            seg_file TEXT,
            seg_file_ind INT,
            rep_count INT
        );"""
        callback = lambda: self.cursor.execute(query)
        self.execute(callback)

    def check_hash_table_exists(self):
        query = """
            SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = 'hash_table'
        );"""
        callback = lambda: self.cursor.execute(query)
        self.execute(callback)
        return self.cursor.fetchone()[0]
    
    def find_hash(self, hash):
        query = """
            SELECT *
            FROM hash_table
            WHERE hash = %s
        ;"""
        callback = lambda: self.cursor.execute(query, (hash,))
        self.execute(callback)
        result = self.cursor.fetchone()
        return result
    
    def find_id(self, id):
        query = """
            SELECT *
            FROM hash_table
            WHERE id = %s
        ;"""
        callback = lambda: self.cursor.execute(query, (id,))
        self.execute(callback)
        result = self.cursor.fetchone()
        return result

    def insert_hash(self, hash, seg_file, seg_file_ind):
        query = """
            INSERT INTO hash_table (hash, seg_file, seg_file_ind, rep_count)
            VALUES (%s, %s, %s, %s)
        ;"""
        callback = lambda: self.cursor.execute(query, (hash, seg_file, seg_file_ind, 1))
        self.execute(callback)

    def update_hash(self, id, rep_count):
        query = """
            UPDATE hash_table 
            SET rep_count = %s
            WHERE id = %s
        ;"""
        callback = lambda: self.cursor.execute(query, (rep_count, id))
        self.execute(callback)

    def clear_table(self):
        query = """
        DROP TABLE hash_table
        ;"""
        callback = lambda: self.cursor.execute(query)
        self.execute(callback)
    
    def __del__(self):
        self.cursor.close()
        self.db.close()


class Deduplicator():
    def __init__(self, seg_size=SEGMENT_SIZE, hash_func=HASH_FUNC, seg_file_size=SEGMENTS_FILE_SIZE, id_size=ID_SIZE):
        self.db = DB()

        self.seg_size = seg_size
        self.seg_file_size = seg_file_size
        self.id_size = id_size

        self.cur_seg_file = ""
        self.seg_num_in_cur_file = 0
        self.cur_file_arr = []

        if hash_func == "md5":
            self.get_segment_hash = lambda x: hashlib.md5(x).hexdigest()
        elif hash_func == "sha1":
            self.get_segment_hash = lambda x: hashlib.sha1(x).hexdigest()
        elif hash_func == "sha512":
            self.get_segment_hash = lambda x: hashlib.sha512(x).hexdigest()
        elif hash_func == "sha256":
            self.get_segment_hash = lambda x: hashlib.sha256(x).hexdigest()
        else:
            self.get_segment_hash = lambda x: x.hex()

    def split_into_segments(self, file_path):
        with open(file_path, "rb") as fo:
            while True:
                segment = fo.read(self.seg_size)
                if len(segment) == 0:
                        break
                yield segment

    def write_segment(self, seg):
        self.cur_file_arr.append(seg)

        if self.seg_num_in_cur_file == 0:
            self.cur_seg_file = "segments_" + datetime.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3] + '.bin'
        seg_num = self.seg_num_in_cur_file

        self.seg_num_in_cur_file += 1
        if self.seg_num_in_cur_file >= self.seg_file_size:
            with open(os.path.join(SEGMENTS_PATH, self.cur_seg_file), "wb") as fs:
                for seg_a in self.cur_file_arr:
                    fs.write(seg_a)
            self.cur_file_arr = []
            self.seg_num_in_cur_file = 0

        return seg_num

    def write_compressed_file(self, file_path, file_as_segment_ids):
        with open(os.path.join(COMPRESSED_DATA_PATH, file_path + '.bin'), "wb") as fc:
            for elem in file_as_segment_ids:
                try:
                    fc.write(elem.to_bytes(self.id_size, byteorder='big', signed=False))
                except (OverflowError):
                    logger.error(f"Id value {elem} is too big, please increase ID_SIZE")
                    logger.error(f"Current ID_SIZE is {self.id_size}")
                    exit(-1)

    def run(self, file_path):
        start_ts = time.time()

        if not os.path.exists(os.path.join(DATA_PATH, file_path)):
            logger.error("File does not exist")
            exit(-1)
    
        self.db.create_hash_table()

        file_as_segs_ids = []
        total_segs_num = 0
        repeated_segs_num = 0
        for seg in self.split_into_segments(os.path.join(DATA_PATH, file_path)):
            hash = self.get_segment_hash(seg)
            hash_row = self.db.find_hash(hash)

            # if segment is new
            if hash_row is None:
                # write new segment into segments files
                seg_num = self.write_segment(seg)

                # insert new segment into db and get its id
                self.db.insert_hash(hash, self.cur_seg_file, seg_num)
                id = self.db.find_hash(hash)[0]

            # if the segment has already been met
            else:
                id = hash_row[0]
                rep_count = hash_row[4]
                self.db.update_hash(id, rep_count + 1)

                repeated_segs_num += 1
            file_as_segs_ids.append(id)
            total_segs_num += 1

        # write file as sequence of segments ids
        self.write_compressed_file(file_path, file_as_segs_ids)

        # write segment file
        with open(os.path.join(SEGMENTS_PATH, self.cur_seg_file), "wb") as fs:
                for seg_a in self.cur_file_arr:
                    fs.write(seg_a)
        self.cur_file_arr = []
        self.seg_num_in_cur_file = 0
        
        avg_time = time.time() - start_ts
        
        logger.info(f"Compressed file          : {file_path}")
        logger.info(f"Total number of segments : {total_segs_num}")
        logger.info(f"Hash table increased by  : {total_segs_num - repeated_segs_num}")
        logger.info(f"Repeated segments        : {repeated_segs_num}")
        logger.info(f"Process time (sec)       : {avg_time}")
        logger.info("-----")

        return total_segs_num, repeated_segs_num, avg_time


class Duplicator():
    def __init__(self, seg_size=SEGMENT_SIZE, id_size=ID_SIZE):
        self.db = DB()
        self.seg_size = seg_size
        self.id_size = id_size

    def read_compressed_file(self, file_path):
        with open(file_path, "rb") as fc:
            while True:
                id = fc.read(self.id_size)
                if len(id) == 0:
                    break
                yield int.from_bytes(id, byteorder="big", signed=False)

    def read_segment_from_file(self, seg_file, seg_file_ind):
        if not os.path.exists(os.path.join(SEGMENTS_PATH, seg_file)):
            return None
        with open(os.path.join(SEGMENTS_PATH, seg_file), "rb") as fs:
            fs.seek(seg_file_ind * self.seg_size)
            seg = fs.read(self.seg_size)
            return seg

    def run(self, file_path):
        start_ts = time.time()
        if not self.db.check_hash_table_exists():
            logger.error("Hash table does not exist")
            exit(-1)
        if not os.path.exists(os.path.join(COMPRESSED_DATA_PATH, file_path)):
            logger.error("File does not exist")
            exit(-1)
    
        orig_file = os.path.splitext(file_path)[0]
        with open(os.path.join(DECOMPRESSED_DATA_PATH, orig_file), "wb") as f:
            for id in self.read_compressed_file(os.path.join(COMPRESSED_DATA_PATH, file_path)):

                hash_row = self.db.find_id(id)
                if id is None:
                    logger.error(f"Hash with id {id} was not found in table")
                    exit(-1)

                seg = self.read_segment_from_file(hash_row[2], hash_row[3])
                if seg is None:
                    logger.error(f"Segment {id} file does not exist")
                    exit(-1)
                
                f.write(seg)

        avg_time = time.time() - start_ts
        logger.info(f"Decompress file    : {orig_file}")
        logger.info(f"Process time (sec) : {avg_time}")
        logger.info("-----")

        return avg_time
    
if __name__ == "__main__":  
    dedup = Deduplicator()
    dup = Duplicator()

    # for file in os.listdir(DATA_PATH):
    #     dedup.run(file)
    # for file in os.listdir(DATA_PATH):
    #     dup.run(file + '.bin')

    file_path = "cat.jpg"
    dedup.run(file_path)
    dup.run(file_path + '.bin')

    file_path = "meow.mp3"
    dedup.run(file_path)
    dup.run(file_path + '.bin')