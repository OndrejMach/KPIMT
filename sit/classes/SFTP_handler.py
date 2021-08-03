import pysftp
import re
from datetime import date, datetime
import os



class SFTP_handler:
    host=None
    username=None
    port=None
    private_key=None
    password=None
    chdir=None
    conn=None

    def __init__(self, host, username,port=22, private_key=None, password=None, chdir=None):
        self.host=host
        self.username=username
        self.port=port
        self.private_key=private_key
        self.password=password
        self.chdir=chdir

    def connect(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.conn = pysftp.Connection(host=self.host, username=self.username, private_key=self.private_key, cnopts=cnopts, port=self.port, password=self.password)
        if (self.chdir):
            self.conn.chdir(self.chdir)
    def set_chdir(self, chdir):
        print("changing working remote dir to "+chdir)
        self.chdir =chdir
        if (self.chdir):
            try:
                self.conn.chdir(self.chdir)
            except IOError:
                print("folder does not exists")
                return False
        return True

    def get_wrong_files(self):
        def testFileFormat(filename, today):
            return re.match("^ICSS\\.DATOS\\.A638IPPR\\.{}\\.CSV$".format(today), filename) or \
                   re.match("^ICSS\\.DATOS\\.A651PSBB\\.{}\\.CSV$".format(today), filename) or \
                   re.match("^ICSS\\.DATOS\\.A652PSVO\\.{}\\.CSV$".format(today), filename) or \
                   re.match("^ICSS\\.DATOS\\.A653RABA\\.{}\\.CSV$".format(today), filename)

        files = self.conn.listdir()
        today = date.today().strftime("%Y%m%d")
        return [x for x in files if not testFileFormat(x, today)]


    def upload(self, local_folder):
        for item in os.listdir(local_folder):
            if (os.path.isfile(os.path.join(local_folder, item)) and (not os.path.isdir(os.path.join(local_folder, item)))):
                print("uploading file "+item +" from " + local_folder)
                self.conn.put(os.path.join(local_folder, item))

    def get(self,local_path):
        for entry in self.conn.listdir():
            print("downloading file " + entry + " to " + local_path)
            self.conn.get(remotepath=entry, localpath=local_path+entry)

    def delete(self):
        for entry in self.conn.listdir():
            self.conn.remove(entry)

    def close(self):
        if (self.conn):
            self.conn.close()

    def getTmpFile(self, app_tmp_folder):
        return app_tmp_folder + datetime.now().strftime("%Y%m%d") + "_MIS_SYNC.tmp"