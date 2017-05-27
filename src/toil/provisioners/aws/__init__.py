# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os
from collections import namedtuple
from operator import attrgetter
import datetime
from cgcloud.lib.util import std_dev, mean

from toil.test import runningOnEC2

logger = logging.getLogger(__name__)

ZoneTuple = namedtuple('ZoneTuple', ['name', 'price_deviation'])


def getSpotZone(spotBid, nodeType, ctx):
    return _getCurrentAWSZone(spotBid, nodeType, ctx)


def getCurrentAWSZone():
    return _getCurrentAWSZone()


def _getCurrentAWSZone(spotBid=None, nodeType=None, ctx=None):
    zone = None
    try:
        import boto
        from boto.utils import get_instance_metadata
    except ImportError:
        pass
    else:
        zone = os.environ.get('TOIL_AWS_ZONE', None)
        if not zone and runningOnEC2():
            try:
                zone = get_instance_metadata()['placement']['availability-zone']
            except KeyError:
                pass
        if not zone and spotBid:
            # if spot bid is present, all the other parameters must be as well
            assert bool(spotBid) == bool(nodeType) == bool(ctx)
            # if the zone is unset and we are using the spot market, optimize our
            # choice based on the spot history
            return optimize_spot_bid(ctx=ctx, instance_type=nodeType, spot_bid=float(spotBid))
        if not zone:
            zone = boto.config.get('Boto', 'ec2_region_name')
            if zone is not None:
                zone += 'a'  # derive an availability zone in the region
    return zone


def choose_spot_zone(zones, bid, spot_history):
    """
    Returns the zone to put the spot request based on, in order of priority:

       1) zones with prices currently under the bid

       2) zones with the most stable price

    :param list[boto.ec2.zone.Zone] zones:
    :param float bid:
    :param list[boto.ec2.spotpricehistory.SpotPriceHistory] spot_history:

    :rtype: str
    :return: the name of the selected zone

    >>> from collections import namedtuple
    >>> FauxHistory = namedtuple( 'FauxHistory', [ 'price', 'availability_zone' ] )
    >>> ZoneTuple = namedtuple( 'ZoneTuple', [ 'name' ] )

    >>> zones = [ ZoneTuple( 'us-west-2a' ), ZoneTuple( 'us-west-2b' ) ]
    >>> spot_history = [ FauxHistory( 0.1, 'us-west-2a' ), \
                         FauxHistory( 0.2,'us-west-2a'), \
                         FauxHistory( 0.3,'us-west-2b'), \
                         FauxHistory( 0.6,'us-west-2b')]
    >>> # noinspection PyProtectedMember
    >>> choose_spot_zone( zones, 0.15, spot_history )
    'us-west-2a'

    >>> spot_history=[ FauxHistory( 0.3, 'us-west-2a' ), \
                       FauxHistory( 0.2, 'us-west-2a' ), \
                       FauxHistory( 0.1, 'us-west-2b'), \
                       FauxHistory( 0.6, 'us-west-2b') ]
    >>> # noinspection PyProtectedMember
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'

    >>> spot_history={ FauxHistory( 0.1, 'us-west-2a' ), \
                       FauxHistory( 0.7, 'us-west-2a' ), \
                       FauxHistory( 0.1, "us-west-2b" ), \
                       FauxHistory( 0.6, 'us-west-2b' ) }
    >>> # noinspection PyProtectedMember
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'
   """

    # Create two lists of tuples of form: [ (zone.name, std_deviation), ... ] one for zones
    # over the bid price and one for zones under bid price. Each are sorted by increasing
    # standard deviation values.
    #
    markets_under_bid, markets_over_bid = [], []
    for zone in zones:
        zone_histories = filter(lambda zone_history:
                                zone_history.availability_zone == zone.name, spot_history)
        if zone_histories:
            price_deviation = std_dev([history.price for history in zone_histories])
            recent_price = zone_histories[0].price
        else:
            price_deviation, recent_price = 0.0, bid
        zone_tuple = ZoneTuple(name=zone.name, price_deviation=price_deviation)
        (markets_over_bid, markets_under_bid)[recent_price < bid].append(zone_tuple)

    return min(markets_under_bid or markets_over_bid,
               key=attrgetter('price_deviation')).name


def optimize_spot_bid(ctx, instance_type, spot_bid):
    """
    Check whether the bid is sane and makes an effort to place the instance in a sensible zone.
    """
    spot_history = _get_spot_history(ctx, instance_type)
    if spot_history:
        _check_spot_bid(spot_bid, spot_history)
    zones = ctx.ec2.get_all_zones()
    most_stable_zone = choose_spot_zone(zones, spot_bid, spot_history)
    logger.info("Placing spot instances in zone %s.", most_stable_zone)
    return most_stable_zone


def _check_spot_bid(spot_bid, spot_history):
    """
    Prevents users from potentially over-paying for instances

    Note: this checks over the whole region, not a particular zone

    :param spot_bid: float

    :type spot_history: list[SpotPriceHistory]

    :raises UserError: if bid is > 2X the spot price's average

    >>> from collections import namedtuple
    >>> FauxHistory = namedtuple( "FauxHistory", [ "price", "availability_zone" ] )
    >>> spot_data = [ FauxHistory( 0.1, "us-west-2a" ), \
                      FauxHistory( 0.2, "us-west-2a" ), \
                      FauxHistory( 0.3, "us-west-2b" ), \
                      FauxHistory( 0.6, "us-west-2b" ) ]
    >>> # noinspection PyProtectedMember
    >>> _check_spot_bid( 0.1, spot_data )
    >>> # noinspection PyProtectedMember

    # >>> Box._check_spot_bid( 2, spot_data )
    Traceback (most recent call last):
    ...
    UserError: Your bid $ 2.000000 is more than double this instance type's average spot price ($ 0.300000) over the last week
    """
    average = mean([datum.price for datum in spot_history])
    if spot_bid > average * 2:
        logger.warn("Your bid $ %f is more than double this instance type's average "
                 "spot price ($ %f) over the last week", spot_bid, average)

def _get_spot_history(ctx, instance_type):
    """
    Returns list of 1,000 most recent spot market data points represented as SpotPriceHistory
    objects. Note: The most recent object/data point will be first in the list.

    :rtype: list[SpotPriceHistory]
    """

    one_week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
    spot_data = ctx.ec2.get_spot_price_history(start_time=one_week_ago.isoformat(),
                                               instance_type=instance_type,
                                               product_description="Linux/UNIX")
    spot_data.sort(key=attrgetter("timestamp"), reverse=True)
    return spot_data

ec2FullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="ec2:*")])

s3FullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="s3:*")])

sdbFullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="sdb:*")])

iamFullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="iam:*")])


logDir = '--log_dir=/var/lib/mesos'
leaderArgs = logDir + ' --registry=in_memory --cluster={name}'
workerArgs = '{keyPath} --work_dir=/var/lib/mesos --master={ip}:5050 --attributes=preemptable:{preemptable} ' + logDir

awsUserData = """#cloud-config

write_files:
    - path: "/home/core/volumes.sh"
      permissions: "0777"
      owner: "root"
      content: |
        #!/bin/bash
        set -x
        ephemeral_count=0
        possible_drives="/dev/xvdb /dev/xvdc /dev/xvdd /dev/xvde"
        drives=""
        directories="toil mesos docker"
        for drive in $possible_drives; do
            echo checking for $drive
            if [ -b $drive ]; then
                echo found it
                ephemeral_count=$((ephemeral_count + 1 ))
                drives="$drives $drive"
                echo increased ephemeral count by one
            fi
        done
        if (("$ephemeral_count" == "0" )); then
            echo no ephemeral drive
            for directory in $directories; do
                sudo mkdir -p /var/lib/$directory
            done
            exit 0
        fi
        sudo mkdir /mnt/ephemeral
        if (("$ephemeral_count" == "1" )); then
            echo one ephemeral drive to mount
            sudo mkfs.ext4 -F $drives
            sudo mount $drives /mnt/ephemeral
        fi
        if (("$ephemeral_count" > "1" )); then
            echo multiple drives
            for drive in $drives; do
                dd if=/dev/zero of=$drive bs=4096 count=1024
            done
            sudo mdadm --create -f --verbose /dev/md0 --level=0 --raid-devices=$ephemeral_count $drives # determine force flag
            sudo mkfs.ext4 -F /dev/md0
            sudo mount /dev/md0 /mnt/ephemeral
        fi
        for directory in $directories; do
            sudo mkdir -p /mnt/ephemeral/var/lib/$directory
            sudo mkdir -p /var/lib/$directory
            sudo mount --bind /mnt/ephemeral/var/lib/$directory /var/lib/$directory
        done

coreos:
    update:
      reboot-strategy: off
    units:
    - name: "volume-mounting.service"
      command: "start"
      content: |
        [Unit]
        Description=mounts ephemeral volumes & bind mounts toil directories
        Author=cketchum@ucsc.edu
        Before=docker.service

        [Service]
        Type=oneshot
        Restart=no
        ExecStart=/usr/bin/bash /home/core/volumes.sh

    - name: "toil-{role}.service"
      command: "start"
      content: |
        [Unit]
        Description=toil-{role} container
        Author=cketchum@ucsc.edu
        After=docker.service

        [Service]
        Restart=on-failure
        RestartSec=2
        ExecPre=-/usr/bin/docker rm toil_{role}
        ExecStart=/usr/bin/docker run \
            --entrypoint={entrypoint} \
            --net=host \
            -v /var/run/docker.sock:/var/run/docker.sock \
            -v /var/lib/mesos:/var/lib/mesos \
            -v /var/lib/docker:/var/lib/docker \
            -v /var/lib/toil:/var/lib/toil \
            -v /var/lib/cwl:/var/lib/cwl \
            -v /tmp:/tmp \
            --name=toil_{role} \
            {image} \
            {args}
    - name: "node-exporter.service"
      command: "start"
      content: |
        [Unit]
        Description=node-exporter container
        After=docker.service

        [Service]
        Restart=on-failure
        RestartSec=2
        ExecPre=-/usr/bin/docker rm node_exporter
        ExecStart=/usr/bin/docker run \
            -p 9100:9100 \
            -v /proc:/host/proc \
            -v /sys:/host/sys \
            -v /:/rootfs \
            --name node-exporter \
            --restart always \
            prom/node-exporter:0.12.0 \
            -collector.procfs /host/proc \
            -collector.sysfs /host/sys \
            -collector.filesystem.ignored-mount-points ^/(sys|proc|dev|host|etc)($|/)
        

ssh_authorized_keys:
    - "ssh-rsa {sshKey}"
"""


prometheusConfig = """
    # my global config
    global:
      scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
      evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
      # scrape_timeout is set to the global default (10s).

      # Attach these labels to any time series or alerts when communicating with
      # external systems (federation, remote storage, Alertmanager).
      external_labels:
          monitor: 'codelab-monitor'

    # Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
    rule_files:
      # - "first.rules"
      # - "second.rules"

    # A scrape configuration containing exactly one endpoint to scrape:
    # Here it's Prometheus itself.
    scrape_configs:
      # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
      - job_name: 'prometheus'

        # metrics_path defaults to '/metrics'
        # scheme defaults to 'http'.

        static_configs:
          - targets: ['localhost:9090']
      - job_name: 'toil'
        static_configs:
          - targets: ['172.31.11.166']
      - job_name: 'aws-node-exporter'
        ec2_sd_configs:
          - region: 'us-west-2'
            refresh_interval: 15s
            port: 9100
        relabel_configs:
          - source_labels: ['__meta_ec2_tag_Name']
            action: keep
            regex: '{}'
          - source_labels: ['__meta_ec2_instance_state']
            action: drop
            regex: '.*(stopped|terminated).*'
"""

toilDashboardConfig = """
{
  "__inputs": [
    {
      "name": "DS_PROMETHEUS",
      "label": "prometheus",
      "description": "",
      "type": "datasource",
      "pluginId": "prometheus",
      "pluginName": "Prometheus"
    }
  ],
  "__requires": [
    {
      "type": "grafana",
      "id": "grafana",
      "name": "Grafana",
      "version": "4.1.0"
    },
    {
      "type": "panel",
      "id": "graph",
      "name": "Graph",
      "version": ""
    },
    {
      "type": "datasource",
      "id": "prometheus",
      "name": "Prometheus",
      "version": "1.0.0"
    }
  ],
  "annotations": {
    "list": []
  },
  "editable": true,
  "gnetId": null,
  "graphTooltip": 0,
  "hideControls": false,
  "id": null,
  "links": [],
  "refresh": "30s",
  "rows": [
    {
      "collapse": false,
      "height": 279,
      "panels": [
        {
          "aliasColors": {},
          "bars": false,
          "datasource": "DS_PROMETHEUS",
          "fill": 1,
          "id": 1,
          "legend": {
            "alignAsTable": false,
            "avg": false,
            "current": false,
            "hideEmpty": true,
            "hideZero": false,
            "max": false,
            "min": false,
            "show": true,
            "total": false,
            "values": false
          },
          "lines": true,
          "linewidth": 1,
          "links": [],
          "nullPointMode": "null",
          "percentage": false,
          "pointradius": 5,
          "points": false,
          "renderer": "flot",
          "seriesOverrides": [],
          "span": 6,
          "stack": false,
          "steppedLine": false,
          "targets": [
            {
              "expr": "avg(irate(node_cpu{mode=\\"user\\",job=\\"aws-node-exporter\\"}[5m])) without (cpu)",
              "intervalFactor": 2,
              "legendFormat": "{{instance}}",
              "metric": "node",
              "refId": "A",
              "step": 2
            }
          ],
          "thresholds": [],
          "timeFrom": null,
          "timeShift": null,
          "title": "CPU usage",
          "tooltip": {
            "shared": true,
            "sort": 0,
            "value_type": "individual"
          },
          "type": "graph",
          "xaxis": {
            "mode": "time",
            "name": null,
            "show": true,
            "values": []
          },
          "yaxes": [
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": "1",
              "min": "0",
              "show": true
            },
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            }
          ]
        },
        {
          "aliasColors": {},
          "bars": false,
          "datasource": "DS_PROMETHEUS",
          "fill": 1,
          "id": 2,
          "legend": {
            "avg": false,
            "current": false,
            "max": false,
            "min": false,
            "show": true,
            "total": false,
            "values": false
          },
          "lines": true,
          "linewidth": 1,
          "links": [],
          "nullPointMode": "null",
          "percentage": false,
          "pointradius": 5,
          "points": false,
          "renderer": "flot",
          "seriesOverrides": [],
          "span": 6,
          "stack": false,
          "steppedLine": false,
          "targets": [
            {
              "expr": "node_memory_MemTotal{job=\\"aws-node-exporter\\"} - node_memory_MemFree{job=\\"aws-node-exporter\\"} - node_memory_Buffers{job=\\"aws-node-exporter\\"} - node_memory_Cached{job=\\"aws-node-exporter\\"}",
              "intervalFactor": 2,
              "legendFormat": "{{instance}}",
              "refId": "A",
              "step": 2
            }
          ],
          "thresholds": [],
          "timeFrom": null,
          "timeShift": null,
          "title": "Memory usage",
          "tooltip": {
            "shared": true,
            "sort": 0,
            "value_type": "individual"
          },
          "type": "graph",
          "xaxis": {
            "mode": "time",
            "name": null,
            "show": true,
            "values": []
          },
          "yaxes": [
            {
              "format": "bytes",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            },
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            }
          ]
        }
      ],
      "repeat": null,
      "repeatIteration": null,
      "repeatRowId": null,
      "showTitle": false,
      "title": "Dashboard Row",
      "titleSize": "h6"
    },
    {
      "collapse": false,
      "height": 202,
      "panels": [
        {
          "aliasColors": {},
          "bars": false,
          "datasource": "DS_PROMETHEUS",
          "fill": 1,
          "id": 3,
          "legend": {
            "avg": false,
            "current": false,
            "max": false,
            "min": false,
            "show": true,
            "total": false,
            "values": false
          },
          "lines": true,
          "linewidth": 1,
          "links": [],
          "nullPointMode": "null",
          "percentage": false,
          "pointradius": 5,
          "points": false,
          "renderer": "flot",
          "seriesOverrides": [],
          "span": 6,
          "stack": false,
          "steppedLine": false,
          "targets": [
            {
              "expr": "autoscaler_cur_size",
              "intervalFactor": 2,
              "legendFormat": "Current cluster size",
              "refId": "A",
              "step": 2
            },
            {
              "expr": "autoscaler_desired_size",
              "intervalFactor": 2,
              "legendFormat": "Autoscaler goal",
              "refId": "B",
              "step": 2
            }
          ],
          "thresholds": [],
          "timeFrom": null,
          "timeShift": null,
          "title": "Cluster size",
          "tooltip": {
            "shared": true,
            "sort": 0,
            "value_type": "individual"
          },
          "type": "graph",
          "xaxis": {
            "mode": "time",
            "name": null,
            "show": true,
            "values": []
          },
          "yaxes": [
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            },
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            }
          ]
        },
        {
          "aliasColors": {},
          "bars": false,
          "datasource": "DS_PROMETHEUS",
          "fill": 1,
          "id": 4,
          "legend": {
            "avg": false,
            "current": false,
            "max": false,
            "min": false,
            "show": false,
            "total": false,
            "values": false
          },
          "lines": true,
          "linewidth": 1,
          "links": [],
          "nullPointMode": "null",
          "percentage": false,
          "pointradius": 5,
          "points": false,
          "renderer": "flot",
          "seriesOverrides": [],
          "span": 6,
          "stack": false,
          "steppedLine": false,
          "targets": [
            {
              "expr": "autoscaler_queue_size",
              "intervalFactor": 2,
              "legendFormat": "Queue size",
              "refId": "A",
              "step": 2
            }
          ],
          "thresholds": [],
          "timeFrom": null,
          "timeShift": null,
          "title": "Queue size",
          "tooltip": {
            "shared": true,
            "sort": 0,
            "value_type": "individual"
          },
          "type": "graph",
          "xaxis": {
            "mode": "time",
            "name": null,
            "show": true,
            "values": []
          },
          "yaxes": [
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            },
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            }
          ]
        }
      ],
      "repeat": null,
      "repeatIteration": null,
      "repeatRowId": null,
      "showTitle": false,
      "title": "Dashboard Row",
      "titleSize": "h6"
    },
    {
      "collapse": false,
      "height": 208,
      "panels": [
        {
          "aliasColors": {},
          "bars": true,
          "datasource": "DS_PROMETHEUS",
          "fill": 1,
          "id": 5,
          "legend": {
            "alignAsTable": false,
            "avg": false,
            "current": false,
            "hideEmpty": true,
            "hideZero": true,
            "max": false,
            "min": false,
            "rightSide": false,
            "show": false,
            "total": false,
            "values": false
          },
          "lines": false,
          "linewidth": 1,
          "links": [],
          "nullPointMode": "null",
          "percentage": false,
          "pointradius": 5,
          "points": false,
          "renderer": "flot",
          "seriesOverrides": [],
          "span": 6,
          "stack": true,
          "steppedLine": false,
          "targets": [
            {
              "expr": "(((issued_jobs - completed_jobs) or issued_jobs) - failed_jobs) or (issued_jobs - completed_jobs) or issued_jobs",
              "intervalFactor": 2,
              "legendFormat": "{{job_type}}",
              "refId": "A",
              "step": 2
            },
            {
              "expr": "",
              "intervalFactor": 2,
              "refId": "B"
            }
          ],
          "thresholds": [],
          "timeFrom": null,
          "timeShift": null,
          "title": "Running or queued jobs",
          "tooltip": {
            "shared": false,
            "sort": 2,
            "value_type": "individual"
          },
          "type": "graph",
          "xaxis": {
            "mode": "time",
            "name": null,
            "show": true,
            "values": []
          },
          "yaxes": [
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            },
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            }
          ]
        },
        {
          "aliasColors": {},
          "bars": false,
          "datasource": "DS_PROMETHEUS",
          "fill": 1,
          "id": 6,
          "legend": {
            "avg": false,
            "current": false,
            "max": false,
            "min": false,
            "show": true,
            "total": false,
            "values": false
          },
          "lines": true,
          "linewidth": 1,
          "links": [],
          "nullPointMode": "null",
          "percentage": false,
          "pointradius": 5,
          "points": false,
          "renderer": "flot",
          "seriesOverrides": [],
          "span": 6,
          "stack": false,
          "steppedLine": false,
          "targets": [
            {
              "expr": "failed_jobs",
              "intervalFactor": 2,
              "legendFormat": "{{job_type}}",
              "refId": "A",
              "step": 2
            }
          ],
          "thresholds": [],
          "timeFrom": null,
          "timeShift": null,
          "title": "Failed jobs",
          "tooltip": {
            "shared": true,
            "sort": 0,
            "value_type": "individual"
          },
          "type": "graph",
          "xaxis": {
            "mode": "time",
            "name": null,
            "show": true,
            "values": []
          },
          "yaxes": [
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            },
            {
              "format": "short",
              "label": null,
              "logBase": 1,
              "max": null,
              "min": null,
              "show": true
            }
          ]
        }
      ],
      "repeat": null,
      "repeatIteration": null,
      "repeatRowId": null,
      "showTitle": false,
      "title": "Dashboard Row",
      "titleSize": "h6"
    }
  ],
  "schemaVersion": 14,
  "style": "dark",
  "tags": [],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-15m",
    "to": "now"
  },
  "timepicker": {
    "refresh_intervals": [
      "5s",
      "10s",
      "30s",
      "1m",
      "5m",
      "15m",
      "30m",
      "1h",
      "2h",
      "1d"
    ],
    "time_options": [
      "5m",
      "15m",
      "1h",
      "6h",
      "12h",
      "24h",
      "2d",
      "7d",
      "30d"
    ]
  },
  "timezone": "browser",
  "title": "toil stats",
  "version": 9
}
"""

mtailConfig = """
    gauge autoscaler_cur_size
    gauge autoscaler_desired_size
    gauge autoscaler_queue_size
    counter total_issued_jobs
    counter issued_jobs by job_type
    counter total_completed_jobs
    counter completed_jobs by job_type
    counter total_failed_jobs
    counter failed_jobs by job_type
    counter missing_jobs

    /cluster needs (?P<desired_size>\d+).* nodes of shape .+, from current size of (?P<cur_size>\d+), given a queue size of (?P<queue_size>\d+)/ {
         autoscaler_desired_size = $desired_size
         autoscaler_cur_size = $cur_size
         autoscaler_queue_size = $queue_size
    }

    /Issued job '(?P<job_type>\S+)'/ {
         issued_jobs[$job_type]++
         total_issued_jobs++
    }

    /Job store ID .* with batch system id .* is missing for the 1 time/ {
         missing_jobs++
    }

    /Due to failure we are reducing the remaining retry count of job '(?P<job_type>\S+)'.*with ID.*to (\d+)/ {
         total_failed_jobs++
         failed_jobs[$job_type]++
    }

    /Job ended successfully: '(?P<job_type>\S+)'/ {
         completed_jobs[$job_type]++
         total_completed_jobs++
    }
"""

