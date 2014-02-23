"""testing rsync protocol (from perl version File::RsyncP)"""

import struct
import stat
import os
from subprocess import Popen
from subprocess import PIPE
from StringIO import StringIO

MAXPATHLEN = 1024

XMIT_TOP_DIR = 1 << 0
XMIT_SAME_MODE = 1 << 1
XMIT_EXTENDED_FLAGS = 1 << 2
XMIT_SAME_RDEV_pre28 = XMIT_EXTENDED_FLAGS
XMIT_SAME_UID = 1 << 3
XMIT_SAME_GID = 1 << 4
XMIT_SAME_NAME = 1 << 5
XMIT_LONG_NAME = 1 << 6
XMIT_SAME_TIME = 1 << 7
XMIT_SAME_RDEV_MAJOR = 1 << 8
XMIT_HAS_IDEV_DATA = 1 << 9
XMIT_SAME_DEV = 1 << 10
XMIT_RDEV_MINOR_IS_SMALL = 1 << 11

# These flags are used in the live flist data.

FLAG_TOP_DIR = 1 << 0
FLAG_HLINK_EOL = 1 << 1     # generator only
FLAG_MOUNT_POINT = 1 << 2   # sender only
FLAG_USER_BOOL = 1 << 12    # for File::RsyncP partials

# the length of the md4 checksum
MD4_SUM_LENGTH = 16
SUM_LENGTH = 16
SHORT_SUM_LENGTH = 2
BLOCKSUM_BIAS = 10


class RsyncP:
    """Rsync protocol"""

    file_list = ''
    rsync_fd = ''
    data_chunk = ''

    protocol_version = 28

    def __init__(self, remote_send=True):
        print 'Initialising RsyncP'
        self.rsync_cmd = '/usr/bin/rsync'
        self.remote_cmd = [self.rsync_cmd]
        self.remote_dir = [
            './test_dir',
            # '../../cfengine',
            # '/dev/random',
        ]

        # define server mode
        self.remote_cmd.append("--server")

        # remote host send to local host
        if remote_send:
            self.remote_cmd.append("--sender")

        self.rsync_args = {
            'numeric-ids': True,
            'rsh': '.',
            'perms': True,
            'owner': True,
            'group': True,
            'devices': True,
            'links': True,
            'copy-links': False,
            'hard-links': True,
            'ignore-times': False,
            'block-size': 700,
            'relative': False,
            'recursive': True,
            'verbose': True,
            'itemize-changes': False,
            'protect-args': False,
            # "-re.iLsf", ".",
            'filter': False,
        }

        for arg, value in self.rsync_args.iteritems():
            if value is False or value is None:
                pass
            elif value is True:
                self.remote_cmd.append('--%s' % arg)
            else:
                self.remote_cmd.append('--%s=%s' % (arg, value))

        if remote_send:
            self.remote_cmd.append('.')
            self.remote_cmd.extend(self.remote_dir)
        else:
            self.remote_cmd.append('.')

        self.block_size = self.rsync_args['block-size']
        if 'timeout' in self.rsync_args:
            self.timeout = self.rsync_args['timeout']
        if 'protocol' in self.rsync_args:
            self.protocol_version = self.rsync_args['protocol']
        # self.fio_version = 1
        # if self.fio is None:
        #     self.fio = FileIO(block_size=self.block_size,
        #                       log_level=self.log_level,
        #                       protocol_version=self.protocol_version,
        #                       preserve_hard_links=self.rsync_args['hard-links'],
        #                       client_charset=self.client_charset,
        #                       )
        #     self.fio_version = self.fio.version
        # else:
        #     self.fio_version = self.fio.version
        #     self.fio.block_Size(self.block_size)
        #     if self.fio_version >= 2:
        #         self.fio.protocol_version(self.protocol_version)
        #         self.fio.preserve_hard_links(self.rsync_args['hard-links'])

    def _read_buff(self, length):
        """read length from buffer string"""
        print 'readin %s bytes' % length
        return self.rsync_fd.stdout.read(length)

    def remote_start(self):
        """start remote rsync"""
        print 'Running: %r' % self.remote_cmd
        self.rsync_fd = Popen(self.remote_cmd, stdin=PIPE, stdout=PIPE)

        # send protocol version
        self.rsync_fd.stdin.write(struct.pack('L', self.protocol_version))

        # get version
        read_data = self._read_buff(4)
        print 'read_data: %r' % (read_data)
        remote_version = struct.unpack('I', read_data)[0]
        print 'remote version: %r' % (remote_version)
        if remote_version < 20 or remote_version > 40:
            print 'Fatal error (bad version): %r' % remote_version
            exit(1)

        # get checksum seed
        read_data = self._read_buff(4)
        print 'read_data: %r' % (read_data)
        checksum_seed = struct.unpack('I', read_data)[0]
        print 'checksum seed: %r' % (checksum_seed)

    def recieved_file_list(self):
        """recieved and parse file list from rsync"""
        self.file_list = FileList(self.rsync_fd)
        if 'owner' in self.rsync_args:
            self.file_list.preserve_uid = self.rsync_args['owner']
        if 'group' in self.rsync_args:
            self.file_list.preserve_gid = self.rsync_args['group']
        if 'links' in self.rsync_args:
            self.file_list.preserve_links = self.rsync_args['links']
        if 'devices' in self.rsync_args:
            self.file_list.preserve_devices = self.rsync_args['devices']
        if 'hard-links' in self.rsync_args:
            self.file_list.preserve_hard_links = self.rsync_args['hard-links']
        if 'checksum' in self.rsync_args:
            self.file_list.always_checksum = self.rsync_args['checksum']
        self.file_list.protocol_version = self.protocol_version

        # send exclude
        self.rsync_fd.stdin.write(struct.pack('L', 0))

        self.file_list.recieve()

    def terminate(self):
        """terminate rsync process"""
        for file_entry in self.file_list.files:
            print 'file: %r' % file_entry
        self.rsync_fd.terminate()


class FileList:
    """filelist class"""
    preserve_uid = True             # --owner
    preserve_gid = True             # --group
    preserve_links = True           # --links
    preserve_devices = True         # --devices
    preserve_hard_links = False     # --hard-links
    always_checksum = False         # --checksum
    remote_version = 28             # remote protocol version
    protocol_version = 28           # locate protocol version
    files = []                      # files
    fatal_error = False
    error_count = 0
    lastname = ''
    lastdir = ''

    def __init__(self, rsync_fd):
        self.rsync_fd = rsync_fd
        self.data_chunk = ''
        self.data_chunk_buffer = ''

    def _read_byte(self):
        """read byte from buffer string"""
        read_data = self._read_buff(1)
        print 'read_data: %r' % (read_data)
        return struct.unpack('B', read_data)[0]

    def _read_int(self):
        """read int from buffer string"""
        read_data = self._read_buff(4)
        print 'read_data: %r' % (read_data)
        return struct.unpack('I', read_data)[0]

    def _read_longint(self):
        """read long int from buffer string"""
        ret = self._read_int()
        if ret != int(0xffffffff):
            print 'return int not longint: %r' % ret
            return ret
        read_data = self._read_buff(8)
        print 'read_data: %r' % (read_data)
        ret = struct.unpack('Q', read_data)[0]
        return ret

    def _read_flags(self):
        """read flags from buffer"""
        flags = self._read_byte()
        if self.protocol_version >= 28 and (flags & XMIT_EXTENDED_FLAGS):
            flags = flags | (self._read_byte() << 8)
        print 'flags: %r' % (flags)
        return flags

    def _read_buff(self, length):
        """read length from buffer string"""
        print 'reading %s bytes from chunk buffer' % length
        return self.data_chunk_buffer.read(length)

    def _read_rsync_buff(self, length):
        """read length from buffer string"""
        print 'reading %s bytes from rsync buffer' % length
        return self.rsync_fd.stdout.read(length)

    def _read_chunk(self, length=1):
        """read chunk of data"""
        while len(self.data_chunk) < length:
            read_data = self._read_rsync_buff(4)
            if not read_data:
                print '_read_chunk: no data'
                return -1
            data = struct.unpack('I', read_data)[0]
            code = (data >> 24) - 7
            print 'code: %r' % code
            length = data & 0xffffff
            print 'len: %r' % length
            read_data = self._read_rsync_buff(length)

            if code == 0:
                self.data_chunk = self.data_chunk + read_data
                print 'length of data_chunk: %r' % len(self.data_chunk)
                # print 'data_chunk: %r' % self.data_chunk
            else:
                read_data = read_data.rstrip('\n\r')
                print read_data
                if code == 1 or read_data.rfind('file has vanished:'):
                    self.error_count = self.error_count + 1

    @classmethod
    def _from_wire_mode(cls, mode):
        """return mode"""
        if (mode & (stat.S_IFREG)) == 0120000 and (stat.S_IFLNK != 0120000):
            return (mode & ~(stat.S_IFREG)) | stat.S_IFLNK
        return mode

    @classmethod
    def _clean_file_name(cls, filename):
        """
        Turns multiple adjacent slashes into a single slash, gets rid of "./"
        elements (but not a trailing dot dir), removes a trailing slash, and
        optionally collapses ".." elements (except for those at the start of the
        string).  If the resulting name would be empty, change it into a ".".
        """
        filename = filename.replace('//', '/')
        filename = filename.replace('./', '')
        if filename.endswith('/'):
            filename = filename.rstrip('/')
        if filename == '':
            return '.'
        return filename

    @classmethod
    def _is_device(cls, mode):
        """return true if device"""
        if stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
            return True
        else:
            return False

    def recieve(self):
        """recevie file list"""
        # get entries
        self._read_chunk(1)
        # self._read_chunk(len(self.data_chunk) + 1)
        self.data_chunk_buffer = StringIO(self.data_chunk)

        # get flags
        flags = self._read_flags()
        while flags:
            #### get entry
            print '*****   START OF FILE   *****'
            file_entry = File()

            # get file path
            if (flags & XMIT_SAME_NAME):
                print 'XMIT_SAME_NAME'
                path_len = self._read_byte()
            else:
                path_len = 0
            print 'path_len: %r' % (path_len)

            # get file path length
            if (flags & XMIT_LONG_NAME):
                file_path_length = self._read_int()
                print 'XMIT_LONG_NAME'
            else:
                file_path_length = self._read_byte()
            print 'file_path_length: %r' % file_path_length

            thisname = self.lastname[:path_len]
            print 'thisname: %r' % (thisname)
            # get file name
            read_data = self._read_buff(file_path_length)
            print 'read_data: %r' % (read_data)
            thisname = thisname + self._clean_file_name(read_data)
            print 'clean thisname: %s' % (thisname)

            self.lastname = thisname

            if '/' in thisname:
                print '/ in thisname'
                file_entry.basename = thisname[thisname.rfind('/') + 1:]
                lastdir_len = len(self.lastdir)
                dirname_len = len(thisname) - len(file_entry.basename) - 1
                print 'lastdir_len %r' % lastdir_len
                print 'dirname_len %r' % dirname_len
                print 'thisname[:lastdir_len] %r' % thisname[:dirname_len]
                print 'self.lastdir %r' % self.lastdir
                if lastdir_len == dirname_len \
                        and self.lastdir == thisname[:dirname_len]:
                    print 'equal dir !!!!'
                    file_entry.dirname = self.lastdir
                else:
                    file_entry.dirname = thisname[:dirname_len]
                    self.lastdir = file_entry.dirname
            else:
                file_entry.basename = thisname
                file_entry.dirname = ''
                self.lastdir = self.lastname

            print 'new dirname: %r' % (file_entry.dirname)
            print 'new basename: %r' % (file_entry.basename)

    # if ((basename = strrchr(thisname, '/')) != NULL) {
    #     dirname_len = ++basename - thisname; /* counts future '\0' */
    #     if (lastdir_len == dirname_len - 1
    #         && strncmp(thisname, lastdir, lastdir_len) == 0) {
    #             dirname = lastdir;
    #             dirname_len = 0; /* indicates no copy is needed */
    #     } else
    #             dirname = thisname;
    # } else {
    #     basename = thisname;
    #     dirname = NULL;
    #     dirname_len = 0;
    # }
    # basename_len = strlen(basename) + 1; /* count the '\0' */

    # if (dirname_len) {
    #     file->dirname = lastdir = bp;
    #     lastdir_len = dirname_len - 1;
    #     memcpy(bp, dirname, dirname_len - 1);
    #     bp += dirname_len;
    #     bp[-1] = '\0';
    #     if (f->sanitize_paths)
    #         lastdir_depth = count_dir_elements(lastdir);
    # } else if (dirname) {
    #     file->dirname = dirname;
    # }

    # file->basename = bp;
    # memcpy(bp, basename, basename_len);
    # bp += basename_len;

            # get file size
            file_entry.size = self._read_longint()
            print 'file size: %rB' % (file_entry.size)
            print 'file size: %rMB' % (file_entry.size / 1024 / 1024)

            # get file mtime
            if not (flags & XMIT_SAME_TIME):
                file_entry.mtime = self._read_int()
                print 'not XMIT_SAME_TIME'
            else:
                file_entry.mtime = self.files[-1].mtime
            print 'mtime: %r' % file_entry.mtime

            # get file mode
            if not (flags & XMIT_SAME_MODE):
                print 'not XMIT_SAME_MODE'
                file_entry.mode = self._from_wire_mode(self._read_int())
            else:
                file_entry.mode = self.files[-1].mode
            print 'mode: %r' % file_entry.mode

            # get file uid
            if self.preserve_uid and not (flags & XMIT_SAME_UID):
                file_entry.uid = self._read_int()
            else:
                file_entry.uid = self.files[-1].uid
            print 'uid: %r' % file_entry.uid

            # get file gid
            if self.preserve_gid and not (flags & XMIT_SAME_GID):
                file_entry.gid = self._read_int()
            else:
                file_entry.gid = self.files[-1].gid
            print 'gid: %r' % file_entry.gid

            if self.preserve_devices:
                if self.protocol_version < 28:
                    print 'protocol_version < 28'
                    if self._is_device(file_entry.mode):
                        if not (flags & XMIT_SAME_RDEV_pre28):
                            rdev = self._read_int()
                            print 'rdev: %r' % rdev
                    else:
                        rdev = os.makedev(0, 0)
                        print 'rdev: %r' % rdev
                elif self._is_device(file_entry.mode):
                    print "is a device!"
                    if not (flags & XMIT_SAME_RDEV_MAJOR):
                        rdev_major = self._read_int()
                        print 'rdev_major: %r' % rdev_major
                    if flags & XMIT_RDEV_MINOR_IS_SMALL:
                        rdev_minor = self._read_byte()
                        print 'rdev_minor: %r' % rdev_minor
                    else:
                        rdev_minor = self._read_int()
                        print 'rdev_minor: %r' % rdev_minor
                    rdev = os.makedev(rdev_major, rdev_minor)
                    print 'rdev: %r' % rdev

            if self.preserve_links and stat.S_ISLNK(file_entry.mode):
                print 'preserve_links and S_ISLNK'
                linkname_length = self._read_int()
                print 'linkname_length: %r' % linkname_length
                if linkname_length <= 0 or linkname_length > MAXPATHLEN:
                    print 'overflow on symlink: linkname_length=%d\n' % \
                        (linkname_length)
                    self.fatal_error = True
                    return
            else:
                linkname_length = None

            if self.always_checksum and stat.S_ISREG(file_entry.mode):
                sum_len = MD4_SUM_LENGTH
            else:
                sum_len = 0

            if flags & XMIT_TOP_DIR:
                file_entry.flags = FLAG_TOP_DIR
            else:
                file_entry.flags = 0

            if self.preserve_devices and self._is_device(file_entry.mode):
                file_entry.rdev = rdev

            if linkname_length is not None:
                file_entry.link = self._read_buff(linkname_length)
                print 'link: %r' % file_entry.link
            #     if (f->sanitize_paths)
            #         sanitize_path(bp, bp, "", lastdir_depth);
            #     bp += linkname_length;
            # }

            if self.preserve_hard_links and self.protocol_version < 28 \
                    and stat.S_ISREG(file_entry.mode):
                flags = flags or XMIT_HAS_IDEV_DATA
            if flags & XMIT_HAS_IDEV_DATA:
                print "XMIT_HAS_IDEV_DATA"
                if self.protocol_version < 26:
                    dev = self._read_int()
                    print 'dev: %r' % dev
                    inode = self._read_int()
                    print 'inode: %r' % inode
                else:
                    if not (flags & XMIT_SAME_DEV):
                        dev = self._read_longint()
                        print 'dev: %r' % dev
                    inode = self._read_longint()
                    print 'inode: %r' % inode

                if self.preserve_hard_links:
                    file_entry.inode = inode
                    file_entry.dev = dev

            # if self.always_checksum:
            #     char *sum;
            #     if sum_len == 0:
            #         file->u.sum = sum = bp;
            #         /*bp += sum_len;*/
            #     } else if (f->protocol_version < 28) {
            #         /* Prior to 28, we get a useless set of nulls. */
            #         sum = empty_sum;
            #     } else
            #         sum = NULL;
            #     if (sum) {
            #         read_buf(f, sum, f->protocol_version < 21 ? 2 : MD4_SUM_LENGTH);
            #     }
            # }

            if file_entry.dirname == '':
                file_entry.name = file_entry.basename
            else:
                file_entry.name = file_entry.dirname + '/' + \
                    file_entry.basename

            self.files.append(file_entry)

            print '*****   END OF FILE - %r   *****' % file_entry.name
            # get flags
            flags = self._read_flags()


class File:
    name = None     # path name of the file (relative to rsync dir): dirname/basename
    basename = None  # file name, without directory
    dirname = None  # directory where file resides
    sum = None      # file MD4 checksum (only present if --checksum specified)
    uid = None      # file user id
    gid = None      # file group id
    mode = None     # file mode
    mtime = None    # file modification time
    size = None     # file length
    dev = None      # device number on which file resides
    inode = None    # file inode
    link = None     # link contents if the file is a sym link
    rdev = None     # major/minor device number if file is char/block special

    flags = 0

    def __repr__(self):
        return '<File name:\'%s\' uid:%i gid:%i mode:%i mtime:%i size:%d>' % \
            (self.name, self.uid, self.gid, self.mode, self.mtime, self.size)

if __name__ == "__main__":
    # import resource
    # print resource.getrusage(resource.RUSAGE_SELF)
    rsyncp = RsyncP()
    rsyncp.remote_start()
    rsyncp.recieved_file_list()
    rsyncp.terminate()
    # print resource.getrusage(resource.RUSAGE_SELF)

