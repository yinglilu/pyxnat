# mirror.py
# pyxnat support for local mirror of ConnectomeDB-hosted data
#
# Copyright (c) 2013 Washington University School of Medicine
# Author: Kevin A. Archie <karchie@wustl.edu>

import types
import errno, os, os.path
from boto.s3.connection import S3Connection

class Mirror(object):
    """Local mirror of Connectome-hosted data.

    Required subclass methods:
    paths_for_subject(subject_label, subdir=None)
      Returns local paths for all files in the named (optional) subdirectory
      for the named subject. Downloads data as necessary.
    """

    def __init__(self, local_root_path):
        """
        Parameters
        ----------
        local_path: path of local mirror
        """
        self.root = local_root_path


class S3Mirror(Mirror):
    """Local mirror of Connectome-hosted data downloaded via S3"""

    def __init__(self, bucket, local_root_path):
        """
        Parameters
        ----------
        bucket: S3 bucket
        local_root_path: path of local mirror
        """
        Mirror.__init__(self, local_root_path)
        self.bucket = bucket


    @classmethod
    def open(cls, bucket, access_key, secret_key, local_root_path):
         return S3Mirror(S3Connection(access_key, secret_key).get_bucket(bucket),
                         local_root_path)

    def get_paths(self, root=None):
        paths = []
        if root and not root.endswith('/'):
            root = root + '/'
        for k in self.bucket.list(root):
            dir = os.path.dirname(os.path.join(self.root,k.name))
            try:
                os.makedirs(dir)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(dir):
                    pass
                else: raise
            path = os.path.join(self.root,k.name)
            try:
                with open(path): pass
            except IOError:
                with open(path, 'w') as f:
                    k.get_contents_to_file(f)
            finally: paths.append(path)
        return paths

    def paths_for_subject(self, subject, subdir=None):
        return self.get_paths(os.path.join(*filter(None, ['q1',subject,subdir])))
