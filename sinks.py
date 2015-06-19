from __future__ import print_function
from io import BytesIO
import os.path
import os
from boto.s3.key import Key

class FileSink(object):
    """This is a sink that wraps a Python file
    """
    def __init__(self, filename):
        try:
            os.makedirs(os.path.dirname(os.path.abspath(filename)))
        except OSError, e:
            pass
        self.file = open(filename, 'w')
    def open(self):
        pass
    def write(self, string):
        self.file.write(string)
    def flush(self):
        self.file.flush()
    def close(self):
        self.file.close()
        self.file = None

def makeS3Sink(conn, bucket_path):
    """Sink that wraps an S3 object
    """
    class S3Sink(object):
        def __init__(self, pathname):
            self.bucket = conn.lookup(bucket_path)
            if not self.bucket:
                self.bucket = conn.create_bucket(bucket_path)
            self.key = Key(self.bucket, pathname)
            self.io = BytesIO()
        def open(self):
            self.close()
        def write(self, string):
            self.io.write(string)
        def close(self):
            if len(self.io.getvalue()):
                self.key.set_contents_from_string(self.io.getvalue())
                self.io = BytesIO()
    return S3Sink
def makeRecordSink(FileType):
    class RecordSink(object):
        """This is a file-like object for writing JSON arrays to file.  Each
        string (record) sent to it by write() is written to the file it
        manages, prepended with a ",\n" if not the first record.  On
        open/close, it writes the JSON "[" and "]" needed to denote an
        array.
    
        """
        def __init__(self, filename):
            self.file = FileType(filename)
            self.first = True
        
        def open(self):
            self.file.write('[\n')
        def write(self, string):
            if not self.first:
                self.file.write(',\n')
            else:
                self.first = False
            self.file.write(string)
        def flush(self):
            self.file.flush()
        def close(self):
            if self.file:
                self.file.write('\n]\n')
                self.file.close()
                self.file = None
    return RecordSink

def makeRollingSink(FileType):
    class RollingSink(object):
        """This is a file-like object for writing to a "rolling" set of files,
        with a user-specified bound on the number of records to write to
        each actual file.
    
        """
        def __init__(self, file_format, rec_limit):
            """`file_format` is a format string that contains "{0}" to indicate
            where the file count should be emitted in the file name
    
            """
            self.file = None
            self.file_format = file_format
            self.rec_limit = rec_limit
            self.file_count = 0
            self.rec_count = 0
            
        def open(self):
            self.file = FileType(self.file_format.format(self.file_count))
            self.file.open()
            self.file_count += 1
        def write(self, string):
            self._roll()
            self.file.write(string)
            self.rec_count += 1
        def flush(self): 
            self.file.flush()
            self._roll()
        def close(self):
            if self.file: 
                self.file.close()
                self.file = None
        
        def _roll(self):
            if self.rec_count > 0 and self.rec_count >= self.rec_limit:
                self.close()
                self.rec_count = 0
            if not self.file:
                self.open()
    return RollingSink

def main():
    def check(test, f, exp):
        act = f.readline()
        if act != exp:
            print("{0}: Expected {1}, got {2}".format(test, exp, act))
    from contextlib import closing
    print('FileSink: testing')
    with closing(FileSink('./foo/bar/baz')) as f:
        f.open()
        if not os.path.exists('./foo/bar/baz'):
            print("FileSink: FileSink('./foo/bar/baz') failed")
        f.write('This is a test\n')
        f.flush()
    with open('./foo/bar/baz') as f:
        check('FileSink', f, 'This is a test\n')
    os.remove('./foo/bar/baz')

    print('RecordSink: testing')
    with closing(makeRecordSink(FileSink)('./foo/bar/0')) as f:
        f.open()
        f.write('"Record 1"')
        f.write('"Record 2"')
        f.write('"Record 3"')
        f.write('"Record 4"')
    with open('./foo/bar/0') as f:
        check('RecordSink', f, '[\n')
        check('RecordSink', f, '"Record 1",\n')
        check('RecordSink', f, '"Record 2",\n')
        check('RecordSink', f, '"Record 3",\n')
        check('RecordSink', f, '"Record 4"\n')
        check('RecordSink', f, ']\n')
    os.remove('./foo/bar/0')
    print('RollingSink: testing')
    with closing(makeRollingSink(makeRecordSink(FileSink))('./foo/bar/{0}', 2)) as f:
        f.open()
        f.write('"Record 1"')
        f.write('"Record 2"')
        f.write('"Record 3"')
        f.write('"Record 4"')
    with open('./foo/bar/0') as f:
        check('RollingSink', f, '[\n')
        check('RollingSink', f, '"Record 1",\n')
        check('RollingSink', f, '"Record 2"\n')
        check('RollingSink', f, ']\n')
    os.remove('./foo/bar/0')
    with open('./foo/bar/1') as f:
        check('RollingSink', f, '[\n')
        check('RollingSink', f, '"Record 3",\n')
        check('RollingSink', f, '"Record 4"\n')
        check('RollingSink', f, ']\n')
    os.remove('./foo/bar/1')
    os.removedirs('./foo/bar')

    print('S3Sink: testing')
    from boto.s3.connection import S3Connection
    conn = S3Connection('AKIAIGBBG3DZFM4MTI4A','9dbOzRaVesmWePrmyX2bFx+T2EA3EHGQWYpvgneP')
    with closing(makeRollingSink(makeRecordSink(makeS3Sink(conn, 'nkrishna-mids205')))('/foo/bar/{0}',2)) as f:
        f.open()
        f.write('"Record 1"')
        f.write('"Record 2"')
        f.write('"Record 3"')
        f.write('"Record 4"')

    b = conn.lookup('nkrishna-mids205')
    k = Key(b, '/foo/bar/0')
    f = BytesIO()
    k.get_contents_to_file(f)
    if f.getvalue() != '[\n"Record 1",\n"Record 2"\n]\n':
        print("S3Sink: unexpected file 0 contents: {0}".format(f.getcontents()))
    k.delete()
    k = Key(b, '/foo/bar/1')
    f = BytesIO()
    k.get_contents_to_file(f)
    if f.getvalue() != '[\n"Record 3",\n"Record 4"\n]\n':
        print("S3Sink: unexpected file 1 contents: {0}".format(f.getcontents()))
    k.delete()


if __name__ == '__main__':
    main()
