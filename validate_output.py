import argparse
import bz2
import glob
import json
import logging
import os

def validate_file(file_path):
    file_name = str.split(file_path, '/')[-1]
    logging.info("Validating %s", file_name)

    # get expected index and type
    info = str.split(file_name, '.')[0]
    dtype = str.split(info, '_')[-1]
    index = '_'.join(str.split(info, '_')[0:-1])

    #parse file
    with open(file_path, 'r') as f_handle:
        data = None
        try:
            data = json.loads(bz2.decompress(f_handle.read()))
        except IOError | ValueError:
            print "ERROR Failed to parse compressed JSON: %s" % file_name
            return False

        for doc in data:
            msg = "ERROR in %s (%s)..." % (file_name, doc["_id"])
            err = False
            if doc["_index"] != index:
                msg += "\n\t- Index: %s != %s" % (doc["_index"], index)
                err = True
            if doc["_type"] != dtype:
                msg += "\n\t- Type: %s != %s" % (doc["_type"], dtype)
                err = True
            if err:
                print msg
                return False

    return True

def main(folder):
    output_dir = os.path.abspath(folder)
    logging.info("output dir = %s", output_dir)

    files = glob.glob("%s/%s" % (output_dir, "*.json.bz2"))
    file_cnt = len(files)
    print "Validating %d files..." % file_cnt

    i = 1
    for file_path in files:
        file_name = str.split(file_path, '/')[-1]
        print "[%d/%d] %s... " % (i, file_cnt, file_name),
        if validate_file(file_path):
            print "PASSED"
        else:
            print "FAILED"

        i += 1

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="A script that validates the output of es_dump.py")
    PARSER.add_argument("folder", nargs='?', default="./output", help="destination folder")
    PARSER.add_argument("-v", dest="verbose", action='store_const', const=True,
                        default=False, help="verbose logging")
    ARGS = PARSER.parse_args()

    if ARGS.verbose:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logging.debug(ARGS)

    main(ARGS.folder)
