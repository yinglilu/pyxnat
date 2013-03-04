# package.py
# pyxnat support for ConnectomeDB download packages
# 
# Copyright (c) 2013 Washington University School of Medicine
# Author: Kevin A. Archie <karchie@wustl.edu>

import json
import os
import subprocess

def _join(xs, separator=','):
    """If xs is a string, return it; otherwise, join it with separator."""
    return xs if isinstance(xs,basestring) else separator.join(xs)

def _aspera_connectdir():
    return os.getenv('ASPERA_CONNECTDIR') or os.getenv('HOME') + "/.aspera/connect"

def _aspera_ascp():
    return os.getenv('ASCP') or _aspera_connectdir() + '/bin/ascp';

_xfer_spec_mapping = {
    'remote_user': '--user',
    'min_rate_kbps': '-m',
    'target_rate_kbps': '-l',
    'fasp_port': '-O',
    'ssh_port': '-P',
    'remote_host': '--host',
    'rate_policy': '--policy',
    'token': '-W'
    }

class Packages(object):
    """Download packages"""

    def __init__(self, interface):
        """
        Parameters
        ----------
        interface: :class:`Interface`
          Main interface reference
        """
        self._intf = interface

    def __iter__(self):
        """ Enumerates the package types. """
        return (p['id'] for p in json.loads(self._intf._exec('/spring/download'))['packages'])

    def list(self):
        """ Enumerates the package types. """
        return list(self)

    def _get_xfer_spec(self, subjects, packages, dest=None):
        """ Get an Aspera transfer_spec to download the named
        packages for the named subjects. subjects and packages
        may be a string or an iterable of strings. If destination
        is provided, puts files in that directory; otherwise,
        files are put in working directory."""
        subjects = _join(subjects)
        packages = _join(packages)
        request = '/spring/download?' + \
            _join(['subjects=' + subjects,
                   'package=' + packages],
                  separator='&')
        if dest:
            request += '&destination=' + dest

        return json.loads(self._intf._exec(request, method='POST'))

    def _apply_xfer_spec(self, xfer_spec):
        """ Uses ascp to perform the download described in xfer_spec."""
        command = [_aspera_ascp(), '-p']
        command.append('-i')
        command.append(_aspera_connectdir() + '/etc/asperaweb_id_dsa.openssh')
        for key, val in xfer_spec.iteritems():
            if key in _xfer_spec_mapping:
                arg = _xfer_spec_mapping[key]
                if arg.startswith('--'):
                    command.append(arg + '=' + val)
                else:
                    command.append(arg)
                    command.append(str(val))

        if 'direction' in xfer_spec and "receive" != xfer_spec['direction']:
            raise Exception('only ascp receives are supported')
        command.append("--mode=recv")

        for pathm in xfer_spec['paths']:
            command.append(pathm['source'])

        command.append('destination_root' in xfer_spec
                       and xfer_spec['destination_root']
                       or '.')
        subprocess.check_output(command)

    def download(self, subjects, packages, dest=None):
        """ Use the Aspera command-line client to download the named
        packages for the named subjects. subjects and packages may be
        a string or an iterable of strings. If destination is
        provided, puts files in that directory; otherwise, files are
        put in working directory."""
        xfer_spec = self._get_xfer_spec(subjects, packages, dest)
        self._apply_xfer_spec(xfer_spec)
