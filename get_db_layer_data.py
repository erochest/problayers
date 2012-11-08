#!/usr/bin/env python


import fileinput
import sys

import requests


GEOSERVER = 'http://libsvr35.lib.virginia.edu:8080/geoserver/rest'
GEOSERVER_AUTH = ('slabadmin', 'GIS4slab!')


def rest_req(uri, method='get', headers=None, data=None):
    method_fn = getattr(requests, method)
    headers = headers or {}
    data = data or {}

    r = method_fn(uri, auth=GEOSERVER_AUTH, headers=headers, data=data)
    r.raise_for_status()
    return r


def rest_text(uri, *args, **kwargs):
    return rest_req(uri, *args, **kwargs).text


def rest_json(uri, *args, **kwargs):
    return rest_req(uri, *args, **kwargs).json


def geoserver(request, *args, **kwargs):
    return rest_json(GEOSERVER + request, *args, **kwargs)


def to_dict(cxn_params):
    """\
    This takes the @key / $ encoding of the connection parameters and returns a
    dict.
    """
    return dict( (p['@key'], p['$']) for p in cxn_params['entry'] )


class DataStore(object):

    def __init__(self, workspace_name, data_store):
        self.workspace_name = workspace_name
        self.data_store = data_store
        self.__feature_type_cache = {}

    def __getitem__(self, key):
        return self.data_store[key]

    def _load_feature_types(self):
        ft_url = self.data_store['featureTypes']
        return dict(
                (ft['name'], ft['href'])
                for ft in rest_json(ft_url)['featureTypes']['featureType']
                )

    @property
    def feature_types(self):
        try:
            fts = self.__feature_types
        except AttributeError:
            self.__feature_types = fts = self._load_feature_types()
        return fts


def get_dstore(workspace_name, cache):
    dstore = cache.get(workspace_name)
    if dstore is None:
        ds_url = '/workspaces/{0}/datastores/{0}.json'.format(workspace_name)
        cache[workspace_name] = dstore = DataStore(
                workspace_name, geoserver(ds_url)['dataStore']
                )
    return dstore


def main():
    dstores = {}

    for line in fileinput.input():
        (workspace_name, layer_name) = line.strip().split('.')
        dstore = get_dstore(workspace_name, dstores)
        cxn_params = to_dict(dstore['connectionParameters'])

        if (cxn_params['dbtype'] == 'postgis' and
            cxn_params['host'] == 'lon.lib.virginia.edu'):
            db_name = cxn_params['database']
            fts = dstore.feature_types
            layer_uri = fts.get(layer_name)
            if layer_uri is not None:
                layer_uri = layer_uri.replace('.json', '.xml')
                layer_data = rest_text(layer_uri)
                out_file = 'layers/{0}.{1}.xml'.format(
                        db_name, layer_name,
                        )
                sys.stderr.write(
                        'writing data for {0}.{1} => {2} ({3} bytes).\n'.format(
                            db_name, layer_name, out_file, len(layer_data),
                            ))
                with open(out_file, 'wb') as fout:
                    fout.write(layer_data)
                if layer_data:
                    sys.stdout.write(line)


if __name__ == '__main__':
    main()
