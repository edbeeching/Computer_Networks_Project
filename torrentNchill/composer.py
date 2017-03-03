import hashlib
import os
"""
    Created by Edward Beeching 24/01/2017
    The function create_orchestra(...) will generate an orchestra file (.file). For a give input file
    By default the output file is the input filename with .orch appended and the part size is 16*2014 Bytes

    .orch files contain:
    #   Conductor
    #   name of composition
    #   size of the composition
    #   size of each part
    #   number of parts
    #   SHA1 checksum of each part
"""


def _get_split_checksums(file_name, part_size=16 * 1024):
    print("Starting split with chucks of %s Byes in size" % part_size)
    chunk_sums = {}
    try:
        with open(file_name, "rb") as file:
            try:
                part_num = 1
                bytes_read = file.read(part_size)
                while bytes_read:

                    hasher = hashlib.sha1()
                    hasher.update(bytes_read)
                    chunk_sums[part_num] = (hasher.hexdigest(), len(bytes_read))
                    # print('part', part_num, len(bytes_read),hasher.hexdigest())
                    bytes_read = file.read(part_size)
                    part_num += 1
            except IOError as er:
                print(er)
            finally:
                file.close()

        return chunk_sums
    except IOError as er:
        print(er)


def _get_file_size(file_name):
    try:
        return os.path.getsize(file_name)
    except TypeError as er:
        print(er)
        return -1


def _get_file_checksum(file_name):
    try:
        with open(file_name, "rb") as file:
            try:
                hasher = hashlib.sha1()
                file_bytes = file.read(_get_file_size(file_name))
                hasher.update(file_bytes)
                return hasher.hexdigest()
            except IOError as er:
                print(er)
            finally:
                file.close()
    except IOError as er:
        print(er)


def create_orchestra(input_file_name, output_file_name=None, part_size=16 * 1024, conductor='localhost:9999'):
    # Create:
    #   Conductor
    #   name of composition
    #   size of the composition
    #   size of each part
    #   number of parts
    #   SHA1 checksum of each part

    if output_file_name is None:
        output_file_name = input_file_name + ".orch"
    try:
        with open(output_file_name, "w") as output:
            try:
                output.write(conductor + "\n")
                output.write(input_file_name + "\n")

                whole_check_sum = _get_file_checksum(input_file_name)
                output.write(str(whole_check_sum) + "\n")
                file_size = _get_file_size(input_file_name)
                output.write(str(file_size) + "\n")
                output.write(str(part_size) + "\n")
                checksums = _get_split_checksums(input_file_name, part_size=part_size)
                output.write(str(len(checksums)) + "\n")
                for part_key in check_sums:
                    (checksum, _) = checksums[part_key]
                    output.write(checksum + "\n")
            except IOError as er:
                print(er)
            finally:
                output.close()
    except IOError as er:
        print(er)


if __name__ == "__main__":

    partsize = 16 * 1024  # 16KB chunk size
    filename = "files/Sciences.M1ML.complete.zip"
    print(_get_file_checksum(filename))
    check_sums = _get_split_checksums(filename, partsize)
    try:

        for key in check_sums:
            (check, size) = check_sums[key]
            print(key, check, size)
    except TypeError as er:
        print(er)
        exit()
    print(check_sums)
    print(_get_file_size(filename))
    create_orchestra(filename)
