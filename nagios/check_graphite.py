#!/usr/bin/env python

"""
Nagios Plugin that reads data from graphite.

Prerequisite:
  easy_install nagaconda

Orginal script from
https://github.com/recoset/check_graphite/blob/master/check_graphite
https://github.com/bitprophet/check_graphite/blob/master/check_graphite

Original copyright:
# Copyright (c) 2011 Recoset <nicolas@recoset.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
"""
import math
import urllib2

from NagAconda import Plugin

graphite = Plugin("Plugin to retrieve data from graphite", "1.0")
graphite.add_option("u", "url", "URL to query for data", required=False)
graphite.add_option("m", "minute", "period of data to get", required=False)
graphite.add_option("-hM", "hostMafia", "host of Mafia server", required=False)
graphite.add_option("-kM", "keyMafia", "key string", required=False)

graphite.enable_status("warning")
graphite.enable_status("critical")
graphite.start()

url = graphite.options.url
if not url:
    url = ''.join([
        'http://localhost/render?format=raw',
        '&from=-', graphite.options.minute, 'minutes',
        '&target=servers.', graphite.options.hostMafia.replace('.', '_'),
            '_9400.', graphite.options.keyMafia,
        ])

try:
    usock = urllib2.urlopen(url)
    data = usock.read()
    usock.close()
    assert data

    pieces = data.split("|")
    counter = pieces[0].split(",")[0]
    values = pieces[1].split(",")[:-1]
    values = map(lambda x: 0.0 if x == 'None' else float(x), values)
    assert not any(map(math.isnan, values))
    avg = sum(values)/len(values);

    graphite.set_value(counter, avg)
    graphite.set_status_message("Avg value of %s was %f" % (counter, avg))

    graphite.finish()
except Exception, e:
    graphite.unknown_error("Error: %s" % e)

