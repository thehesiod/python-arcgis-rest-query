import aiohttp
import ujson
import asyncio
import logging
from urllib.parse import urljoin


class ArcGIS:
    """
    A class that can download a layer from a map in an
    ArcGIS web service and convert it to something useful,
    like GeoJSON.

    Usage:

    >>> import arcgis
    >>> source = "http://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/USA_Congressional_Districts/FeatureServer"
    >>> arc = arcgis.ArcGIS(source)
    >>> layer_id = 0
    >>> shapes = arc.get(layer_id, "STATE_ABBR='IN'")

    This assumes you've inspected your ArcGIS services endpoint to know what to look for.
    ArcGIS DOES publish json files enumerating the endpoints you can query, so autodiscovery
    could be possible further down the line.

    """
    def __init__(self, url: str, geom_type=None, object_id_field: str="OBJECTID",
                 username: str=None, password: str=None,
                 token_url: str='https://www.arcgis.com/sharing/rest/generateToken',
                 logger: logging.Logger=None):

        if not url.endswith("/"):
            url += '/'

        self.url = url
        self.object_id_field = object_id_field
        self._layer_descriptor_cache = {}
        self.geom_type = geom_type
        self._geom_parsers = {
            'esriGeometryPoint': self._parse_esri_point,
            'esriGeometryMultipoint': self._parse_esri_multipoint,
            'esriGeometryPolyline': self._parse_esri_polyline,
            'esriGeometryPolygon': self._parse_esri_polygon
        }

        self.username = username
        self.password = password
        self.token_url = token_url
        self._token = None
        self._http_session = None
        self._logger = logger

    async def __aenter__(self):
        self._http_session = await aiohttp.ClientSession().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._http_session.__aexit__(exc_type, exc_val, exc_tb)

    def _build_request(self, layer):
        url = urljoin(self.url, layer)
        if not url.endswith('/'):
            url += '/'h
        return url

    def _build_query_request(self, layer):
        return urljoin(self._build_request(layer), "query")

    @staticmethod
    def _parse_esri_point(geom):
        return {
            "type": "Point",
            "coordinates": [
                geom.get('x'),
                geom.get('y')
            ]
        }

    @staticmethod
    def _parse_esri_multipoint(geom):
        return {
            "type": "MultiPoint",
            "coordinates": geom.get('points')
        }

    @staticmethod
    def _parse_esri_polyline(geom):
        return {
            "type": "MultiLineString",
            "coordinates": geom.get('paths')
        }

    @staticmethod
    def _parse_esri_polygon(geom):
        return {
            "type": "Polygon",
            "coordinates": geom.get('rings')
        }

    def _determine_geom_parser(self, type):
        return self._geom_parsers.get(type)

    @staticmethod
    def esri_to_geojson(obj, geom_parser):
        return {
            "type": "Feature",
            "properties": obj.get('attributes'),
            "geometry": geom_parser(obj.get('geometry')) if obj.get('geometry') else None
        }

    async def get_json(self, layer, where: str="1 = 1", fields: list=None, count_only: bool=False, srid: str='4326'):
        """
        Gets the JSON file from ArcGIS
        """
        if fields is None:
            fields = list()

        params = {
                'where': where,
                'outFields': ", ".join(fields),
                'returnGeometry': "True",
                'outSR': srid,
                'f': "pjson",
                'orderByFields': self.object_id_field,
                'returnCountOnly': "True" if count_only else "False"
        }

        if await self.token:
            params['token'] = await self.token
        if self.geom_type:
            params.update({'geometryType': self.geom_type})

        url = self._build_query_request(layer)
        async with self._http_session.get(url, params=params) as response:
            if response.status not in {200, 204}:
                raise Exception("Invalid status: {} for url: {} and params: {}".format(response.status, url, params))

            data = await response.json(loads=ujson.loads)
            if data.get('error'):
                raise Exception("Invalid response: {} for url: {} and params: {}".format(data, url, params))

            return data

    async def get_descriptor_for_layer(self, layer):
        """
        Returns the standard JSON descriptor for the layer. There is a lot of
        usefule information in there.
        """
        if layer not in self._layer_descriptor_cache:
            params = {'f': 'pjson'}
            if await self.token:
                params['token'] = await self.token

            async with self._http_session.get(self._build_request(layer), params=params) as response:
                if response.status not in {200, 204}:
                    raise Exception("Invalid status: {}".format(response.status))

                response = await response.json(loads=ujson.loads)

            self._layer_descriptor_cache[layer] = response
        return self._layer_descriptor_cache[layer]

    async def enumerate_layer_fields(self, layer):
        """
        Pulls out all of the field names for a layer.
        """
        descriptor = await self.get_descriptor_for_layer(layer)
        return [field['name'] for field in descriptor['fields']]

    async def get(self, layer, where: str="1 = 1", fields: list=None, count_only: bool=False, srid: str='4326'):
        """
        Gets a layer and returns it as honest to God GeoJSON.

        WHERE 1 = 1 causes us to get everything. We use OBJECTID in the WHERE clause
        to paginate, so don't use OBJECTID in your WHERE clause unless you're going to
        query under 1000 objects.
        """

        if fields is None:
            fields = list()

        base_where = where
        # By default we grab all of the fields. Technically I think
        # we can just do "*" for all fields, but I found this was buggy in
        # the KMZ mode. I'd rather be explicit.
        fields = fields or await self.enumerate_layer_fields(layer)

        jsobj = await self.get_json(layer, where, fields, count_only, srid)

        # Sometimes you just want to know how far there is to go.
        if count_only:
            return jsobj.get('count')

        # If there is no geometry, we default to assuming it's a Table type
        # data format, and we dump a simple (non-geo) json of all of the data.
        if not jsobj.get('geometryType', None):
            return await self.getTable(layer, where, fields, jsobj=jsobj)

        # From what I can tell, the entire layer tends to be of the same type,
        # so we only have to determine the parsing function once.
        geom_parser = self._determine_geom_parser(jsobj.get('geometryType'))

        features = []
        # We always want to run once, and then break out as soon as we stop
        # getting exceededTransferLimit.
        while True:
            features += [self.esri_to_geojson(feat, geom_parser) for feat in jsobj.get('features')]
            if jsobj.get('exceededTransferLimit', False) is False:
                break

            # If we've hit the transfer limit we offset by the last OBJECTID
            # returned and keep moving along.
            where = "%s > %s" % (self.object_id_field, features[-1]['properties'].get(self.object_id_field))
            if base_where != "1 = 1" :
                # If we have another WHERE filter we needed to tack that back on.
                where += " AND %s" % base_where
            jsobj = await self.get_json(layer, where, fields, count_only, srid)

        return {
            'type': "FeatureCollection",
            'features': features
        }

    async def getTable(self, layer, where="1 = 1", fields=[], jsobj=None):
        """
        Returns JSON for a Table type. You shouldn't use this directly -- it's
        an automatic falback from .get if there is no geometry
        """
        base_where = where
        features = []

        # We always want to run once, and then break out as soon as we stop
        # getting exceededTransferLimit.
        while True:
            features += [feat.get('attributes') for feat in jsobj.get('features')]
            # There isn't an exceededTransferLimit?
            if len(jsobj.get('features')) < 1000:
                break

            # If we've hit the transfer limit we offset by the last OBJECTID
            # returned and keep moving along.
            where = "%s > %s" % (self.object_id_field, features[-1].get(self.object_id_field))
            if base_where != "1 = 1":
                # If we have another WHERE filter we needed to tack that back on.
                where += " AND %s" % base_where
            jsobj = await self.get_json(layer, where, fields)
        return features

    async def getMultiple(self, layers, where="1 = 1", fields=[], srid='4326', layer_name_field=None):
        """
        Get a bunch of layers and concatenate them together into one. This is useful if you
        have a map with layers for, say, every year named stuff_2014, stuff_2013, stuff_2012. Etc.

        Optionally, you can stuff the source layer name into a field of your choosing.

        >>> arc.getMultiple([0, 3, 5], layer_name_field='layer_src_name')

        """
        features = []
        for layer in layers:
            get_fields = fields or await self.enumerate_layer_fields(layer)
            this_layer = await self.get(layer, where, get_fields, False, srid).get('features')
            if layer_name_field:
                descriptor = await self.get_descriptor_for_layer(layer)
                layer_name = descriptor.get('name')
                for feature in this_layer:
                    feature['properties'][layer_name_field] = layer_name
            features += this_layer
        return {
            'type': "FeatureCollection",
            'features': features
        }

    @property
    async def token(self):
        if self._token is None and self.username and self.password:
            token_params = {
                'f': 'json',
                'username': self.username,
                'password': self.password,
                'expiration': 60,
                'client': 'referer',
                'referer': 'http://www.arcgis.com',
            }
            try:
                async with self._http_session.post(self.token_url, data=token_params) as response:
                    if response.status not in {200, 204}:
                        raise Exception("Invalid status: {}".format(response.status))

                    response = await response.json(loads=ujson.loads)

                self._token = response.get('token')
                if self._token is None:
                    raise Exception("Missing Token from response")
            except (TimeoutError, asyncio.TimeoutError):
                msg = 'Connection to {0} timed out'.format(self.token_url)
                if self._logger:
                    self._logger.exception(msg)
                else:
                    print(msg)
                raise
            except aiohttp.DisconnectedError:
                msg = 'Unable to connect to host at {0}'.format(self.token_url)
                if self._logger:
                    self._logger.exception(msg)
                else:
                    print(msg)
                raise
            except:
                msg = 'Error getting token from url: {0}'.format(self.token_url)
                if self._logger:
                    self._logger.exception(msg)
                else:
                    print(msg)
                raise

        return self._token
