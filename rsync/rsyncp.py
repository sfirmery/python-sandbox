"""testing rsync protocol (from perl version File::RsyncP)"""

import struct
import stat
import os
from subprocess import Popen
from subprocess import PIPE
from StringIO import StringIO


class RsyncP:
    """Rsync protocol"""
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

    file_list = ''
    buffer = ''
    rsync_fd = ''

    protocol_version = 28

    def __init__(self, remote_send=True):
        print 'Initialising RsyncP'
        self.rsync_cmd = '/usr/bin/rsync'
        self.remote_cmd = [self.rsync_cmd]
        self.remote_dir = [
                           '/Users/sylvain/Documents/Images CD/OpenBSD-install52-amd64.iso',
                           '/Users/sylvain/Documents/Images CD/symlink.iso',
                           '/Users/sylvain/Documents/Images CD/hardlink.iso',
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
            'hard-links': True,
            'ignore-times': False,
            'block-size': 700,
            'relative': False,
            'recursive': True,
            'verbose': True,
            'itemize-changes': False,
            'copy-links': True,
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

    def read_byte(self):
        """read byte from buffer string"""
        read_data = self.buffer.read(1)
        print 'read_data: %r' % (read_data)
        return struct.unpack('B', read_data)[0]

    def read_int(self):
        """read int from buffer string"""
        read_data = self.buffer.read(4)
        print 'read_data: %r' % (read_data)
        return struct.unpack('I', read_data)[0]

    def read_longint(self):
        """read long int from buffer string"""
        read_data = self.buffer.read(4)
        ret = struct.unpack('I', read_data)[0]
        print 'longint ret: %r' % ret
        if ret != -1:
            return ret
        read_data = read_data + self.buffer.read(4)
        ret = struct.unpack('L', read_data)[0]
        print 'longint ret: %r' % ret
        return ret

    def read_buff(self, length):
        """read length from buffer string"""
        print 'readin %s bytes' % length
        return self.buffer.read(length)

    @classmethod
    def from_wire_mode(cls, mode):
        """return mode"""
        if (mode & (stat.S_IFREG)) == 0120000 and (stat.S_IFLNK != 0120000):
            return (mode & ~(stat.S_IFREG)) | stat.S_IFLNK
        return mode

    def remote_start(self):
        """start remote rsync"""
        print 'Running: %r' % self.remote_cmd
        # if __name__ == "__main__":
        # self.rsync = Popen(["/usr/bin/rsync", "--server",
        #                              "--sender", "-re.iLsf", ".",
        #                              "/Users/sylvain/Documents/Images CD/OpenBSD-install52-amd64.iso",
        #                              "/Users/sylvain/Documents/Images CD/Win 2003 srv.iso"
        #                              ],
        #                              # "/Users/sylvain/VirtualBox VMs/OpenBSD/"],
        #                              stdin=PIPE, stdout=PIPE)

        self.rsync_fd = Popen(self.remote_cmd, stdin=PIPE, stdout=PIPE)

        # send protocol version
        self.rsync_fd.stdin.write(struct.pack('L', self.protocol_version))

        # get version
        read_data = self.rsync_fd.stdout.read(4)
        print 'read_data: %r' % (read_data)
        remote_version = struct.unpack('I', read_data)[0]
        print 'remote version: %r' % (remote_version)
        if remote_version < 20 or remote_version > 40:
            print 'Fatal error (bad version): %r' % remote_version
            exit(1)

        # get checksum seed
        read_data = self.rsync_fd.stdout.read(4)
        print 'read_data: %r' % (read_data)
        checksum_seed = struct.unpack('I', read_data)[0]
        print 'checksum seed: %r' % (checksum_seed)

    def recieved_file_list(self):
        """recieved and parse file list from rsync"""
        self.file_list = FileList()
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

        # get filelist header
        read_data = self.rsync_fd.stdout.read(4)
        print 'read_data: %r' % (read_data)
        data = struct.unpack('I', read_data)[0]
        # print "data: %r" % (data)
        # b = [0, 0, 0, 0]
        # b[0] = struct.unpack("b", read_data[0])[0]
        # b[1] = struct.unpack("b", read_data[1])[0]
        # b[2] = struct.unpack("b", read_data[2])[0]
        # b[3] = struct.unpack("b", read_data[3])[0]
        # data2 = int(b[0]) | b[1] << 8 | b[2] << 16 | b[3] << 24
        # print "data2: %r" % (data2)
        print 'code: %r' % ((data >> 24) - 7)
        length = data & 0xffffff
        print 'len: %r' % length

        # get entries
        read_data = self.rsync_fd.stdout.read(length)
        print 'read_data: %r' % (read_data)
        self.buffer = StringIO(read_data)

        while True:
            print '*****   START OF FILE   *****'

            #### get entry
            # get flags
            data = self.read_byte()
            print 'first bit: %r' % (data)
            print 'first bit << 8 (for version >=28): %r' % (data << 8)
            flags = data << 8

            length_1 = 0
            length_2 = 0
            if (flags & self.XMIT_SAME_NAME):
                length_1 = self.read_byte()
                print 'XMIT_SAME_NAME'
                print 'length_1: %r' % length_1

            # get file path length
            if (flags & self.XMIT_LONG_NAME):
                length_2 = self.read_int()
                print 'XMIT_LONG_NAME'
                print 'file path length (l2): %r' % length_2
            else:
                length_2 = self.read_byte()
                print 'file path length (l2): %r' % length_2

            if (length_2 >= self.MAXPATHLEN - length_1):
                print 'overflow: flags=0x%x l1=%d l2=%d' % (flags, length_1, length_2)
                exit(1)

            # get file path
            read_data = self.read_buff(length_2)
            print 'read_data: %r' % (read_data)
            print 'filename: %r' % (read_data)

            # get file_length
            file_length = self.read_longint()
            print 'file length: %rB' % (file_length)
            print 'file length: %rMB' % (file_length / 1024 / 1024)

            if not (flags & self.XMIT_SAME_TIME):
                modtime = self.read_int()
                print 'not XMIT_SAME_TIME'
                print 'modtime: %r' % modtime
            if not (flags & self.XMIT_SAME_MODE):
                print 'not XMIT_SAME_MODE'
                mode = self.from_wire_mode(self.read_int())
                print 'mode: %r' % mode

            # if (f->preserve_uid && !(flags & XMIT_SAME_UID))
            if self.file_list.preserve_uid and not (flags & self.XMIT_SAME_UID):
                uid = self.read_int()
                print 'uid: %r' % uid
            # if (f->preserve_gid && !(flags & XMIT_SAME_GID))
            if self.file_list.preserve_gid and not (flags & self.XMIT_SAME_GID):
                gid = self.read_int()
                print 'gid: %r' % gid

            if self.file_list.preserve_devices:
                if self.file_list.protocol_version < 28:
                    print 'protocol_version < 28'
                    if stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
                        if not (flags & self.XMIT_SAME_RDEV_pre28):
                            rdev = self.read_int()
                    else:
                        rdev = os.makedev(0, 0)
                elif stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
                    print "is a device!"
                    if not (flags & self.XMIT_SAME_RDEV_MAJOR):
                        rdev_major = self.read_int()
                        print 'rdev_major: %r' % rdev_major
                    if flags & self.XMIT_RDEV_MINOR_IS_SMALL:
                        rdev_minor = self.read_byte()
                        print 'rdev_minor: %r' % rdev_minor
                    else:
                        rdev_minor = self.read_int()
                        print 'rdev_minor: %r' % rdev_minor
                    rdev = os.makedev(rdev_major, rdev_minor)

            if self.file_list.preserve_links and stat.S_ISLNK(mode):
                linkname_len = self.read_int() + 1      # count the '\0'
                if linkname_len <= 0 or linkname_len > self.MAXPATHLEN:
                    print 'overflow on symlink: linkname_len=%d\n' % (linkname_len - 1)
                    self.file_list.fatal_error = True
                    return
            else:
                linkname_len = 0

            if self.file_list.always_checksum and stat.S_ISREG(mode):
                sum_len = self.MD4_SUM_LENGTH
            else:
                sum_len = 0

            file_entry = File(modtime=modtime, length=length,
                              mode=mode, uid=uid, gid=gid)
            if flags & self.XMIT_TOP_DIR:
                file_entry.flags = self.FLAG_TOP_DIR
            else:
                file_entry.flags = 0


            print '*****   END OF FILE   *****'

    def terminate(self):
        """terminate rsync process"""
        self.rsync_fd.terminate()


class FileList:
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
    lastname = '\0'

    def __init__(self):
        pass


class File:
    flags = 0
    modtime = 0
    length = 0
    mode = 0
    uid = 0
    gid = 0


if __name__ == "__main__":
    rsyncp = RsyncP()
    rsyncp.remote_start()
    rsyncp.recieved_file_list()
    rsyncp.terminate()

