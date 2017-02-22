import os
import math
import hashlib
# A small program to generate files that are missing parts, for testing

# Lets read the file into memory and then write out a few different files

file_name = 'files/maxresdefault.jpg'
file_size = int()
try:
    file_size = os.path.getsize(file_name)
except IOError as er:
    print(er)


part_size = 16 * 1024
num_parts = math.ceil(file_size / part_size)
byte_buffer = bytearray(file_size)
print('file size is:', file_size)
print('num_parts:', num_parts)

try:
    with open(file_name, 'rb') as file:
        try:
            part_num = 1
            data = file.read(part_size)
            while data:

                start = (part_num - 1)*part_size
                end = min(part_num * part_size, file_size)

                byte_buffer[start:end] = data
                data = file.read(part_size)
                part_num += 1
        except IOError as er:
            print(er)

        finally:
            file.close()
except IOError as er:
    print(er)


for i in range(1, num_parts + 1):
    hasher = hashlib.sha1()
    start = (i - 1) * part_size
    end = min(i * part_size, file_size)
    hasher.update(byte_buffer[start:end])
    print('Checksum', i, hasher.hexdigest())

odds = [1, 3, 5, 7, 9]
evens = [2, 4, 6, 8]

odd_output_file = 'files/maxresdefault_odd_parts.jpg'
odd_byte_buffer = bytearray(file_size)
even_output_file = 'files/maxresdefault_even_parts.jpg'
even_byte_buffer = bytearray(file_size)

for i in odds:
    start = (i - 1) * part_size
    end = min(i * part_size, file_size)
    odd_byte_buffer[start:end] = byte_buffer[start:end]

for i in evens:
    start = (i - 1) * part_size
    end = min(i * part_size, file_size)
    even_byte_buffer[start:end] = byte_buffer[start:end]
try:
    with open(odd_output_file, 'wb') as file:
        try:
            file.write(odd_byte_buffer)
        except IOError as er:
            print(er)
        finally:
            file.close()
except IOError as er:
    print(er)
try:
    with open(even_output_file, 'wb') as file:
        try:
            file.write(even_byte_buffer)
        except IOError as er:
            print(er)
        finally:
            file.close()
except IOError as er:
    print(er)


def check_file(file_name2):
    try:
        with open(file_name2, 'rb') as file2:
            try:

                data2 = file2.read(part_size)
                while data2:

                    hasher2 = hashlib.sha1()
                    hasher2.update(data2)
                    print(hasher2.hexdigest())
                    data2 = file2.read(part_size)
            finally:
                file.close()
    except IOError as er:
        print(er)



print('Checking files')
check_file(odd_output_file)
print('--------------------------')
check_file(even_output_file)








