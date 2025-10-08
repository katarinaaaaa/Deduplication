from deduplication import DB, Deduplicator, Duplicator
from statistics import mean
import os

DATA_PATH = "data/original"
COMPRESSED_DATA_PATH = "data/compressed/files"
DECOMPRESSED_DATA_PATH = "data/decompressed"
SEGMENTS_PATH = "data/compressed/segments"
TEST_RES_PATH = "test_results"


def check_data(orig_file):
    orig_file_path = os.path.join(DATA_PATH, orig_file)
    decomp_file_path = os.path.join(DECOMPRESSED_DATA_PATH, orig_file)
    with open(orig_file_path, "rb") as fo, open(decomp_file_path, "rb") as fd: 
        orig_data = fo.read()
        decomp_data = fd.read()

        err = 0
        for i in range(0, min(len(orig_data), len(decomp_data))):
            if orig_data[i] != decomp_data[i]:
                err += 1
        err += abs(len(orig_data) - len(decomp_data))

    return err


def clear():
    db = DB()
    if db.check_hash_table_exists():
        db.clear_table()
    for file in os.listdir(COMPRESSED_DATA_PATH):
        os.remove(os.path.join(COMPRESSED_DATA_PATH, file))
    for file in os.listdir(SEGMENTS_PATH):
        os.remove(os.path.join(SEGMENTS_PATH, file))
    for file in os.listdir(DECOMPRESSED_DATA_PATH):
        os.remove(os.path.join(DECOMPRESSED_DATA_PATH, file))


def calc_dir_size(path):
    return sum(os.path.getsize(os.path.join(dp, f)) for dp, dn, fn in os.walk(path) for f in fn if os.path.isfile(os.path.join(dp, f)))


def run_test(test_seg_size, test_hash_func, type):
    # clear all previous results and database
    clear()

    dedup = Deduplicator(seg_size=test_seg_size, hash_func=test_hash_func)
    dup = Duplicator(seg_size=test_seg_size)
    
    all_total_segs_num = 0
    all_repeated_segs_num = 0
    dedup_all_avg_time = 0

    if type == "all":
        files_list = os.listdir(DATA_PATH)
    else:
        files_list = []
        for file in os.listdir(DATA_PATH):
            if file.endswith(type):
                files_list.append(file)

    for file in files_list:
        total_segs_num, repeated_segs_num, avg_time = dedup.run(file)
        all_total_segs_num += total_segs_num
        all_repeated_segs_num += repeated_segs_num
        dedup_all_avg_time += avg_time
        print(f'{file} compressed')

    dup_all_avg_time = 0
    for file in os.listdir(COMPRESSED_DATA_PATH):
        avg_time = dup.run(file)
        dup_all_avg_time += avg_time
        print(f'{file} decompressed')

    errors_count = 0
    for file in files_list:
        errors_count += check_data(file)

    segs_reuse_perc = all_repeated_segs_num / all_total_segs_num * 100
    compression_ratio = calc_dir_size(DATA_PATH) / calc_dir_size("data/compressed")
    
    return dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count


def run_test_n_times(n, seg_size, hash_func, type):
    dedup_all_avg_time_arr = []
    segs_reuse_perc_arr = []
    compression_ratio_arr = []
    dup_all_avg_time_arr = []
    errors_count_arr = []

    for i in range(n):
        print(i)
        dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count = run_test(seg_size, hash_func, type)
        dedup_all_avg_time_arr.append(dedup_all_avg_time)
        segs_reuse_perc_arr.append(segs_reuse_perc)
        compression_ratio_arr.append(compression_ratio)
        dup_all_avg_time_arr.append(dup_all_avg_time)
        errors_count_arr.append(errors_count)

    return mean(dedup_all_avg_time_arr), mean(segs_reuse_perc_arr), mean(compression_ratio_arr), mean(dup_all_avg_time_arr), mean(errors_count_arr)
    

def write_results(dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count, test_targ_name, test_targ):
    with open(os.path.join(TEST_RES_PATH, test_targ_name, f'dedup_avg_time.txt'), 'a') as f:
        f.write(f'{test_targ} {dedup_all_avg_time}\n')
    with open(os.path.join(TEST_RES_PATH, test_targ_name, f'segs_reuse_perc.txt'), 'a') as f:
        f.write(f'{test_targ} {segs_reuse_perc}\n')
    with open(os.path.join(TEST_RES_PATH, test_targ_name, f'compression_ratio.txt'), 'a') as f:
        f.write(f'{test_targ} {compression_ratio}\n')
    with open(os.path.join(TEST_RES_PATH, test_targ_name, f'dup_all_avg_time.txt'), 'a') as f:
        f.write(f'{test_targ} {dup_all_avg_time}\n')
    with open(os.path.join(TEST_RES_PATH, test_targ_name, f'errors_count.txt'), 'a') as f:
        f.write(f'{test_targ} {errors_count}\n')

if __name__ == "__main__":

    test_runs_num = 3
    seg_sizes = [4, 10, 20, 50, 100, 500]
    hash_funcs = ["none", "md5", "sha1", "sha256", "sha512"]


    # hash_func = "md5"
    # print("size tests")
    # for size in seg_sizes:
    #     dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count = run_test_n_times(test_runs_num, size, hash_func, "all")
    #     write_results(dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count, "size", size)

    # size = 20
    # print("hash function tests")
    # for hash_func in hash_funcs:
    #     dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count = run_test_n_times(test_runs_num, size, hash_func, "all")
    #     write_results(dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count, "hash", hash_func)

    # size = 20
    # hash_func = "md5"
    # print("file type tests")
    # dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count = run_test_n_times(test_runs_num, size, hash_func, ".txt")
    # write_results(dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count, "types1", "text")

    # dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count = run_test_n_times(test_runs_num, size, hash_func, ".jpg")
    # write_results(dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count, "types1", "photo")

    # dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count = run_test_n_times(test_runs_num, size, hash_func, ".mp3")
    # write_results(dedup_all_avg_time, segs_reuse_perc, compression_ratio, dup_all_avg_time, errors_count, "types1", "audio")

    clear()