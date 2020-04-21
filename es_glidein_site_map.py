#!/usr/bin/env python3
"""
Aggregate site info from ES, and make an html map.
"""


import os
import glob
from optparse import OptionParser
import logging
from functools import partial
from pprint import pprint

parser = OptionParser('usage: %prog [options]')
parser.add_option('-a','--address',help='elasticsearch address')
parser.add_option('-n','--indexname',default='condor',
                  help='index name (default condor)')
parser.add_option('--type',default='all',
                  help='glidein type (all, pyglidein, glideinwms)')
parser.add_option('-o', '--outfile', default='map.html',
                  help='Output html file for map')
(options, args) = parser.parse_args()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

# name: (lat, long)
site_locs = {
  'AGLT2': (42.276, -83.741),
  'AWS': (40.0334, -83.15825),
  'Alberta': (53.5232, -113.5263),
  'BEgrid-ULB-VUB': (50.8333, 4.3333),
  'BNL-ATLAS': (40.8695, -72.8868),
  'Boston': (42.349, -71.104),
  'Bridges': (40.445595, -79.949165),
  'CA-MCGILL-CLUMEQ-T2': (45.4, -73.8),
  'CA-SCINET-T2': (43.8, -79.5),
  'CHTC': (43.071633, -89.406992),
  'CIT_CMS_T2': (34.1379, -118.124),
  'Cedar': (49.2768, -122.9180),
  'Colorado': (40.015, -105.27),
  'Comet': (32.884363, -117.238995),
  'Crane': (40.819655, -96.705694),
  'DESY': (52.345, 13.633),
  'DESY-ZN': (52.345, 13.633),
  'FLTech': (28.077494, -80.61987),
  'GPGrid': (41.85143, -88.242767),
  'GZK': (43.072840, -89.407818),
  'GridUNESP_CENTRAL': (-23.524246, -46.665443),
  'Guillimin': (45.4, -73.8),
  'HEP_WISC': (43.073671, -89.405726),
  'Hyak': (38.6488, -90.3108),
  'IIT_CE1': (41.8349, -87.627),
  'IIT_CE2': (41.8349, -87.627),
  'Illinois': (40.109647, -88.21246),
  'Illume': (53.5232, -113.5263),
  'Indiana': (39.173557, -86.501091),
  'LIDO_Dortmund': (51.4927, 7.4128),
  'MIT': (42.364347, -71.2),
  'MSU': (42.603642, -84.79248),
  'MWT2': (41.790194, -87.599347),
  'Marquette': (43.0385, -87.9304),
  'NBI': (55.696898, 12.571665),
  'NERSC': (37.866825, -122.253582),
  'NUMEP_CE': (42.05408, -87.68809),
  'NWICG_NDCMS': (41.701415, -86.24498),
  'Nebraska': (40.819655, -96.705694),
  'OSG_US_DANFORTH': (41.790194, -87.599347),
  'OSG_US_WSU_GRID': (42.348937, -83.08994),
  'PSU': (40.445595, -79.949165),
  'Purdue-Hadoop': (40.424923, -87.),
  'RWTH-Aachen': (50.7801, 6.0657),
  'SPRACE': (-23.52433, -46.665469),
  'SU-ITS-CE2': (43.0392, -76.1351),
  'SU-ITS-CE3': (43.0392, -76.1351),
  'SU-OG-CE': (43.0392, -76.1351),
  'SU-OG-CE1': (43.0392, -76.1351),
  'SWT2_CPB': (35.207175, -97.447073),
  'Sandhills': (40.819655, -96.705694),
  'Syracuse': (43.0392, -76.1351),
  'T2B_BE_IIHE': (50.83, 4.37),
  'Tusker': (41.247321, -96.016817),
  'UCD': (38.536669, -121.751304),
  'UChicago': (41.790194, -87.599347),
  'UCLA': (34.0689, -118.4452),
  'UCSDT2': (33.016928, -116.846046),
  'UColorado_HEP': (40.015, -105.27),
  'UConn-OSG': (41.777201, -72.217941),
  'UKI-LT2-QMUL': (51.52, -0.04),
  'UKI-NORTHGRID-MAN-HEP': (53.47, -2.23),
  'UMD': (38.833563, -76.877743),
  'USCMS-FNAL-WC1': (41.841368, -88.255057), 
  'UTA_SWT2': (32.771419, -97.291484),
  'Uppsala': (59.8509, 17.6300),
  'Utah': (40.7608, -111.891),
  'Vanderbilt': (36.141621, -86.796477),
  'aachen': (50.7801, 6.0657),
  'aachen2': (50.7801, 6.0657),
  'dockerized_pyglidein': (53.5232, -113.5263),
  'illume-new': (53.5232, -113.5263),
  'mainz': (50., 8.27),
  'msu': (42.603642, -84.79248),
  'osgconnect': (41.790194, -87.599347),
  'parallel': (53.53, -113.53),
  'wuppertalprod': (51.246, 7.149),
  'xsede-osg': (39.173557, -86.501091),
  'xstream': (37.428596, -122.177514),
  'japan': (35.6051, 140.1233)
}

def draw_mpl_map(glideinwms, pyglidein, title='Glidein', outfile='map.png'):
    import numpy as np
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt
    from datetime import datetime
    # miller projection
    map = Basemap(projection='robin',lon_0=0)
    # plot coastlines, draw label meridians and parallels.
    map.drawcoastlines()
    #map.drawparallels(np.arange(-90,90,30),labels=[1,0,0,0])
    #map.drawmeridians(np.arange(map.lonmin,map.lonmax+30,60),labels=[0,0,0,1])
    # fill continents 'coral' (with zorder=0), color wet areas 'aqua'
    #map.drawmapboundary(fill_color='aqua')
    #map.fillcontinents(color='coral',lake_color='aqua')

    # draw lines
    pyglidein['japan'] = 1
    
    for i,s in enumerate(glideinwms):
        if s in site_locs and site_locs[s] != (43.0731, -89.4012):
            try:
                map.drawgreatcircle(-89.4012, 43.0731,
                                  site_locs[s][1], site_locs[s][0],
                                  linewidth=1, color='#40E0D0', label='GlideinWMS' if i==0 else '_nolegend_')
            except Exception as e:
                print(s, site_locs[s], e)
                #raise
                lats = [43.0731, site_locs[s][0]]
                lons = [-89.4012, site_locs[s][1]]
                xx, yy = list(map(lons, lats))  
                map.plot(xx, yy, linewidth=1, color='#40E0D0', label='GlideinWMS' if i==0 else '_nolegend_')
    for i,s in enumerate(pyglidein):
        if s in site_locs and site_locs[s] != (43.0731, -89.4012):
            try:
                map.drawgreatcircle(-89.4012, 43.0731,
                                  site_locs[s][1], site_locs[s][0],
                                  linestyle='--', linewidth=1, color='#FFA500', label='Pyglidein' if i==0 else '_nolegend_')
            except Exception as e:
                print(s, site_locs[s], e)
                #raise
                lats = [43.0731, site_locs[s][0]]
                lons = [-89.4012, site_locs[s][1]]
                xx, yy = list(map(lons, lats))  
                map.plot(xx, yy, linestyle='--', linewidth=1, color='#FFA500', label='Pyglidein' if i==0 else '_nolegend_')
    
    plt.title(title)
    plt.legend(loc='lower center', ncol=2)
    plt.savefig(outfile, dpi=300, bbox_inches='tight')

def draw_google_map(sites, outfile="map.html"):
    pprint(sites)
    output = """<!DOCTYPE html>
<html>
  <head>
    <meta name="viewport" content="initial-scale=1.0, user-scalable=no">
    <meta charset="utf-8">
    <title>Simple Polylines</title>
    <style>
      /* Always set the map height explicitly to define the size of the div
       * element that contains the map. */
      #map {
        height: 100%;
      }
      /* Optional: Makes the sample page fill the window. */
      html, body {
        height: 100%;
        margin: 0;
        padding: 0;
      }
    </style>
  </head>
  <body>
    <div id="map"></div>
    <script>

      // This example creates a 2-pixel-wide red polyline showing the path of
      // the first trans-Pacific flight between Oakland, CA, and Brisbane,
      // Australia which was made by Charles Kingsford Smith.

      function initMap() {
        var map = new google.maps.Map(document.getElementById('map'), {
          zoom: 3,
          center: {lat: 0, lng: 0},
          mapTypeId: 'terrain'
        });
        var pathCoords = [];
        var path;
"""

    makePath = """
        pathCoords = [
          {lat: 43.0731, lng: -89.4012},
          {lat: %f, lng: %f},
        ];
        path = new google.maps.Polyline({
          path: pathCoords,
          geodesic: true,
          strokeColor: '#FF0000',
          strokeOpacity: 1.0,
          strokeWeight: 2
        });

        path.setMap(map);"""

    for s in sites:
        if s in site_locs:
            try:
                output += makePath%(site_locs[s][0], site_locs[s][1])
            except Exception as e:
                print(s, site_locs[s], e)
                raise

    output += """
      }
    </script>
    <script async defer
    src="https://maps.googleapis.com/maps/api/js?key=XXXXXXXXXXXXXXXX&callback=initMap">
    </script>
  </body>
</html>"""
    with open(outfile, 'w') as f:
        f.write(output)

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

prefix = 'http'
address = options.address
if '://' in address:
    prefix,address = address.split('://')

url = '{}://{}'.format(prefix, address)
logging.info('connecting to ES at %s',url)
es = Elasticsearch(hosts=[url],
                   timeout=5000)

queries = {
    'all': '(NOT Owner: pyglidein) AND (NOT Cmd:"/usr/bin/condor_dagman") AND (NOT MATCH_EXP_JOBGLIDEIN_ResourceName: NPX)',
    'glideinwms': '(NOT Owner: pyglidein) AND (NOT Cmd:"/usr/bin/condor_dagman") AND LastRemotePool: glidein2 AND (NOT MATCH_EXP_JOBGLIDEIN_ResourceName: msu)',
    'pyglidein': '(NOT Owner: pyglidein) AND (NOT Cmd:"/usr/bin/condor_dagman") AND (NOT LastRemotePool: glidein2) AND (NOT MATCH_EXP_JOBGLIDEIN_ResourceName: NPX)',
}

def es_agg(query):
    ret = es.search(index=options.indexname, body={
        'query': {
            'query_string': {
                'query': query,
                'analyze_wildcard': True,
            }
        },
        'size': 0,
        'aggs': {
            '1': {
                'terms': {
                    'field': 'MATCH_EXP_JOBGLIDEIN_ResourceName.keyword',
                    'min_doc_count': 1,
                    'size': 100000,
                },
            },
        },
    })

    sites = {}
    for b in ret['aggregations']['1']['buckets']:
        if b['key'] in ('Local Job','TEST','other'):
            continue
        sites[b['key']] = b['doc_count']
    return sites

if options.type in queries:
    print('query:',options.type)
    glideinwms = es_agg(queries['glideinwms'])
    pyglidein = es_agg(queries['pyglidein'])
else:
    raise Exception('bad type')

draw_mpl_map(glideinwms, pyglidein, title='Glidein Locations')
