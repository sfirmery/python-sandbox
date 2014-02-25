"""testing rsync protocol (from perl version File::RsyncP)"""

import struct
import stat
import os
import logging
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

logger = logging.getLogger('rsyncp')

# define logging level
logger.setLevel(logging.DEBUG)
# logger.setLevel(logging.INFO)

if hasattr(logging, 'captureWarnings'):
    # New in Python 2.7
    logging.captureWarnings(True)

# define logging handler
console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
console_format = '%(asctime)s - %(name)s:%(lineno)d(%(funcName)s): %(levelname)s %(message)s'
console_formatter = logging.Formatter(console_format, '%b %d %H:%M:%S')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


class RsyncP:
    """Rsync protocol"""
    file_list = ''
    rsync_fd = ''

    protocol_version = 28

    def __init__(self, remote_send=True):
        self.chunk_data = StringIO()
        self.chunk_len = 0
        self.error_count = 0

        logger.info('Initialising RsyncP')
        self.rsync_cmd = '/usr/bin/rsync'
        self.remote_cmd = [self.rsync_cmd]
        self.remote_dir = [
            # './test_dir',
            # './test_dir/dir1/subdir4567/145file1',
            # '../../cfengine/.git',
            '/Users/sylvain',
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
            'block-size': 512,
            'relative': False,
            'recursive': True,
            'verbose': True,
            'itemize-changes': False,
            'protect-args': False,
            # "-re.iLsf", ".",
            'filter': False,
            'checksum': True,
            'checksum-seed': 75842745, # struct.unpack('I', open("/dev/urandom","rb").read(4))
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
        logger.debug('readin %s bytes', length)
        return self.rsync_fd.stdout.read(length)

    def _read_chunk(self, length=1):
        """read chunk of data"""
        while self.chunk_len < length:
            read_data = self._read_buff(4)
            if not read_data:
                logger.debug('_read_chunk: no data')
                return -1
            data = struct.unpack('I', read_data)[0]
            code = (data >> 24) - 7
            logger.debug('code: %r', code)
            length = data & 0xffffff
            logger.debug('len: %r', length)
            read_data = self._read_buff(length)
            if code == 0:
                read_data = self.chunk_data.read(self.chunk_len) + read_data
                self.chunk_len = self.chunk_len + length
                self.chunk_data.close()
                self.chunk_data = StringIO(read_data)
                logger.debug('length of chunk_data: %r', len(read_data))
                # print 'chunk_data: %r' % self.chunk_data
            else:
                read_data = read_data.rstrip('\n\r')
                logger.error('%s (%s)', read_data, code)
                if code == 1 or read_data.rfind('file has vanished:'):
                    self.error_count = self.error_count + 1

    def remote_start(self):
        """start remote rsync"""
        logger.info('Running: %r', self.remote_cmd)
        self.rsync_fd = Popen(self.remote_cmd, stdin=PIPE, stdout=PIPE)

        # send protocol version
        self.rsync_fd.stdin.write(struct.pack('L', self.protocol_version))

        # get version
        read_data = self._read_buff(4)
        logger.debug('read_data: %r', read_data)
        remote_version = struct.unpack('I', read_data)[0]
        logger.debug('remote version: %r', remote_version)
        if remote_version < 20 or remote_version > 40:
            logger.error('Fatal error (bad version): %r', remote_version)
            exit(1)

        # get checksum seed
        read_data = self._read_buff(4)
        logger.debug('read_data: %r', read_data)
        checksum_seed = struct.unpack('I', read_data)[0]
        logger.debug('checksum seed: %r', checksum_seed)

    def sync(self):
        """sync with remote"""

        self.received_file_list()

        #

        # get file delta
        read_data = self._read_chunk(4)
        logger.debug('read_data: %r', read_data)
        file_num = struct.unpack('I', self.chunk_data)[0]
        logger.debug('file_num: %r', file_num)

    def received_file_list(self):
        """received and parse file list from rsync"""
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
        logger.info('number of files: %s', len(self.file_list.files))
        for file_entry in self.file_list.files:
            logger.debug('file: %r', file_entry)
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
        self.chunk_data = StringIO()
        self.chunk_len = 0
        self.flags = 0

    def _read_byte(self):
        """read byte from buffer string"""
        read_data = self._read_buff(1)
        logger.debug('read_data: %r', read_data)
        return struct.unpack('B', read_data)[0]

    def _read_int(self):
        """read int from buffer string"""
        read_data = self._read_buff(4)
        logger.debug('read_data: %r', read_data)
        return struct.unpack('I', read_data)[0]

    def _read_longint(self):
        """read long int from buffer string"""
        ret = self._read_int()
        if ret != int(0xffffffff):
            logger.debug('return int not longint: %r', ret)
            return ret
        read_data = self._read_buff(8)
        logger.debug('read_data: %r', read_data)
        ret = struct.unpack('Q', read_data)[0]
        return ret

    def _read_buff(self, length):
        """read length from buffer string"""
        logger.debug('need %s bytes from buffer', length)
        if self.chunk_len < length:
            logger.debug('need %s more bytes (%s)',
                         length - self.chunk_len, self.chunk_len)
            self._read_chunk(length)
        logger.debug('reading %s bytes of %s from buffer',
                     length, self.chunk_len)
        self.chunk_len = self.chunk_len - length
        return self.chunk_data.read(length)

    def _read_rsync_buff(self, length):
        """read length from buffer string"""
        logger.debug('reading %s bytes from rsync buffer', length)
        return self.rsync_fd.stdout.read(length)

    def _read_chunk(self, length=1):
        """read chunk of data"""
        while self.chunk_len < length:
            read_data = self._read_rsync_buff(4)
            if not read_data:
                logger.debug('_read_chunk: no data')
                return -1
            data = struct.unpack('I', read_data)[0]
            code = (data >> 24) - 7
            logger.debug('code: %r', code)
            length = data & 0xffffff
            logger.debug('len: %r', length)
            read_data = self._read_rsync_buff(length)
            if code == 0:
                read_data = self.chunk_data.read(self.chunk_len) + read_data
                self.chunk_len = self.chunk_len + length
                self.chunk_data.close()
                self.chunk_data = StringIO(read_data)
                logger.debug('length of chunk_data: %r', len(read_data))
                # print 'chunk_data: %r' % self.chunk_data
            else:
                read_data = read_data.rstrip('\n\r')
                logger.error('%s (%s)', read_data, code)
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

    def _read_flags(self):
        """read flags from buffer"""
        self.flags = self._read_byte()
        if self.protocol_version >= 28 and (self.flags & XMIT_EXTENDED_FLAGS):
            self.flags = self.flags | (self._read_byte() << 8)
        logger.debug('flags: %r', self.flags)

    def _read_paths_len(self):
        """read dir and file path from buffer"""
        # get dir path length
        if (self.flags & XMIT_SAME_NAME):
            logger.debug('XMIT_SAME_NAME')
            dir_len = self._read_byte()
        else:
            dir_len = 0
        logger.debug('dir_len: %r', dir_len)

        # get file path length
        if (self.flags & XMIT_LONG_NAME):
            file_path_len = self._read_int()
            logger.debug('XMIT_LONG_NAME')
        else:
            file_path_len = self._read_byte()
        logger.debug('file_path_len: %r', file_path_len)
        return (dir_len, file_path_len)

    def _read_file_path(self, dir_len, file_path_len):
        """read dir and file path from buffer"""
        thisname = self.lastname[:dir_len]
        logger.debug('thisname: %r', thisname)

        # get file name
        read_data = self._read_buff(file_path_len)
        logger.debug('read_data: %r', read_data)
        thisname = thisname + self._clean_file_name(read_data)
        logger.debug('clean thisname: %s', thisname)

        self.lastname = thisname

        if '/' in thisname:
            basename = thisname[thisname.rfind('/') + 1:]
            lastdir_len = len(self.lastdir)
            dirname_len = len(thisname) - len(basename) - 1
            # logger.debug('lastdir_len %r', lastdir_len)
            # logger.debug('dirname_len %r', dirname_len)
            # logger.debug('thisname[:lastdir_len] %r',
            #              thisname[:dirname_len])
            # logger.debug('self.lastdir %r', self.lastdir)
            if lastdir_len == dirname_len \
                    and self.lastdir == thisname[:dirname_len]:
                dirname = self.lastdir
            else:
                dirname = thisname[:dirname_len]
                self.lastdir = dirname
        else:
            basename = thisname
            dirname = ''
            self.lastdir = self.lastname
        return dirname, basename

    def _read_file_size(self):
        """read file size from buffer"""
        file_size = self._read_longint()
        logger.debug('file size: %rB', file_size)
        logger.debug('file size: %rMB', file_size / 1024 / 1024)
        return file_size

    def _read_mtime(self):
        """read file mtime from buffer"""
        if not (self.flags & XMIT_SAME_TIME):
            mtime = self._read_int()
            logger.debug('not XMIT_SAME_TIME')
        else:
            mtime = self.files[-1].mtime
        logger.debug('mtime: %r', mtime)
        return mtime

    def _read_mode(self):
        """read file mode from buffer"""
        if not (self.flags & XMIT_SAME_MODE):
            logger.debug('not XMIT_SAME_MODE')
            mode = self._from_wire_mode(self._read_int())
        else:
            mode = self.files[-1].mode
        logger.debug('mode: %r', mode)
        return mode

    def _read_uid(self):
        """read uid from buffer"""
        if self.preserve_uid and not (self.flags & XMIT_SAME_UID):
            uid = self._read_int()
        else:
            uid = self.files[-1].uid
        logger.debug('uid: %r', uid)
        return uid

    def _read_gid(self):
        """read gid from buffer"""
        if self.preserve_gid and not (self.flags & XMIT_SAME_GID):
            gid = self._read_int()
        else:
            gid = self.files[-1].gid
        logger.debug('gid: %r', gid)
        return gid

    def _read_link_len(self, mode):
        """read link len from buffer"""
        if self.preserve_links and stat.S_ISLNK(mode):
            logger.debug('preserve_links and S_ISLNK')
            linkname_length = self._read_int()
            logger.debug('linkname_length: %r', linkname_length)
            if linkname_length <= 0 or linkname_length > MAXPATHLEN:
                logger.debug('overflow on symlink: linkname_length=%d\n',
                             linkname_length)
                self.fatal_error = True
                return
        else:
            linkname_length = None
        return linkname_length

    def _read_device_pre28(self, mode):
        """read device from buffer"""
        if self._is_device(mode):
            if not (self.flags & XMIT_SAME_RDEV_pre28):
                rdev = self._read_int()
            else:
                rdev = self.files[-1].rdev
            logger.debug('rdev: %r', rdev)
        else:
            rdev = os.makedev(0, 0)
            logger.debug('rdev: %r', rdev)
        return rdev

    def _read_device(self):
        """read device from buffer"""
        if not (self.flags & XMIT_SAME_RDEV_MAJOR):
            rdev_major = self._read_int()
        else:
            rdev_major = self.files[-1].rdev_major
        logger.debug('rdev_major: %r', rdev_major)

        if self.flags & XMIT_RDEV_MINOR_IS_SMALL:
            rdev_minor = self._read_byte()
        else:
            rdev_minor = self._read_int()
        logger.debug('rdev_minor: %r', rdev_minor)
        return rdev_major, rdev_minor

    def _read_hardlink(self):
        """read hardlink from buffer"""
        if self.protocol_version < 26:
            dev = self._read_int()
            logger.debug('dev: %r', dev)
            inode = self._read_int()
            logger.debug('inode: %r', inode)
        else:
            if self.flags & XMIT_SAME_DEV:
                dev = self.files[-1].dev
            else:
                dev = self._read_longint()
                logger.debug('dev: %r', dev)
            inode = self._read_longint()
            logger.debug('inode: %r', inode)
        return (dev, inode)

    def _read_checksum(self, mode):
        """read checksum from buffer"""
        if not stat.S_ISREG(mode):
            logger.debug('checksum of non regular file')
            checksum = None
        elif self.protocol_version < 21:
            logger.debug('checksum of regular file < 21')
            checksum = self._read_buff(2)
        else:
            logger.debug('checksum of regular file < 28')
            checksum = self._read_buff(MD4_SUM_LENGTH)
        logger.debug('checksum: %r', checksum)
        return checksum

    def _get_file_entry(self):
        """get a file entry from buffer"""
        #### get entry
        logger.debug('*****   START OF FILE   *****')
        file_entry = File()

        # read paths len
        dir_len, file_path_len = self._read_paths_len()

        # read file and dir name
        file_entry.dirname, file_entry.basename = self.\
            _read_file_path(dir_len, file_path_len)

        # get file size
        file_entry.size = self._read_file_size()

        # get file mtime
        file_entry.mtime = self._read_mtime()

        # get file mode
        file_entry.mode = self._read_mode()

        # get file uid
        file_entry.uid = self._read_uid()

        # get file gid
        file_entry.gid = self._read_gid()

        if self.preserve_devices:
            if self.protocol_version < 28:
                logger.debug('protocol_version < 28')
                file_entry.rdev = self._read_device_pre28(file_entry.mode)
            elif self._is_device(file_entry.mode):
                logger.debug('is a device!')
                file_entry.rdev_major, file_entry.rdev_minor = self.\
                    _read_device()
                file_entry.rdev = os.makedev(file_entry.rdev_major,
                                             file_entry.rdev_minor)
                logger.debug('rdev: %r', file_entry.rdev)

        # read link length
        linkname_length = self._read_link_len(file_entry.mode)

        if self.flags & XMIT_TOP_DIR:
            file_entry.flags = self.flags & FLAG_TOP_DIR
        else:
            file_entry.flags = self.flags & 0

        if linkname_length is not None:
            file_entry.link = self._read_buff(linkname_length)
            logger.debug('link: %r', file_entry.link)
        #     if (f->sanitize_paths)
        #         sanitize_path(bp, bp, "", lastdir_depth);
        #     bp += linkname_length;
        # }

        if self.preserve_hard_links and self.protocol_version < 28 \
                and stat.S_ISREG(file_entry.mode):
            self.flags = self.flags | XMIT_HAS_IDEV_DATA

        # read hardlink
        if self.flags & XMIT_HAS_IDEV_DATA:
            logger.debug('XMIT_HAS_IDEV_DATA')
            file_entry.dev, file_entry.inode = self._read_hardlink()

        # read checksum
        if self.always_checksum:
            file_entry.checksum = self._read_checksum(file_entry.mode)

        if file_entry.dirname == '':
            file_entry.name = file_entry.basename
        else:
            file_entry.name = file_entry.dirname + '/' + \
                file_entry.basename

        self.files.append(file_entry)
        logger.debug('*****   END OF FILE - %r   *****', file_entry.name)

    def recieve(self):
        """recevie file list"""
        # get entries

        # get flags
        self._read_flags()
        while self.flags:
            # get file entry for each flags
            self._get_file_entry()

            # get next flags
            self._read_flags()


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
    rdev_major = 0     # major device number if file is char/block special
    rdev_minor = 0     # minor device number if file is char/block special
    rdev = None     # major/minor device number if file is char/block special
    checksum = None

    flags = 0

    def __repr__(self):
        return '<File name:\'%s\' uid:%i gid:%i mode:%i mtime:%i size:%d>' % \
            (self.name, self.uid, self.gid, self.mode, self.mtime, self.size)

if __name__ == "__main__":
    # import cProfile
    # import resource
    # print resource.getrusage(resource.RUSAGE_SELF)
    rsyncp = RsyncP()
    rsyncp.remote_start()
    rsyncp.sync()
    # cProfile.run('rsyncp.sync()')
    rsyncp.terminate()
    # print resource.getrusage(resource.RUSAGE_SELF)
