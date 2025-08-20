"use strict";
/*
 * Copyright 2016 Teppo Kurki <teppo.kurki@iki.fi>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
Object.defineProperty(exports, "__esModule", { value: true });
var _a = require('./skToInflux'), deltaToPointsConverter = _a.deltaToPointsConverter, influxClientP = _a.influxClientP, pruneTimestamps = _a.pruneTimestamps;
var HistoryAPI_1 = require("./HistoryAPI");
module.exports = function (app) {
    var logError = app.error || (function (err) { console.error(err); });
    var clientP;
    var selfContext = 'vessels.' + app.selfId;
    var started = false;
    var unsubscribes = [];
    var shouldStore = function (path) {
        return true;
    };
    function toMultilineString(influxResult) {
        var currentLine = [];
        var result = {
            type: 'MultiLineString',
            coordinates: []
        };
        influxResult.forEach(function (row) {
            if (row.position === null && row.jsonPosition == null) {
                currentLine = [];
            }
            else {
                var position = void 0;
                if (row.position) {
                    position = JSON.parse(row.position);
                }
                else {
                    position = JSON.parse(row.jsonPosition);
                    position = [position.longitude, position.latitude];
                }
                if (position[0] !== 0 && position[1] !== 0) {
                    currentLine[currentLine.length] = position;
                    if (currentLine.length === 1) {
                        result.coordinates[result.coordinates.length] = currentLine;
                    }
                }
            }
        });
        return result;
    }
    function timewindowStart() {
        return new Date(new Date().getTime() - 60 * 60 * 1000).toISOString();
    }
    function getContextClause(pathElements) {
        var skPath = null;
        var contextClause = '';
        if (pathElements != null) {
            if (pathElements.length == 1) {
                contextClause = "and context =~ /" + pathElements[0] + ".*/";
            }
            else if (pathElements.length > 1) {
                var context = pathElements[1];
                if (context == 'self') {
                    context = app.selfId;
                }
                contextClause = "and context =~ /" + pathElements[0] + "." + context + ".*/";
            }
            skPath = pathElements.slice(2).join('.');
            if (skPath.length === 0) {
                skPath = null;
            }
        }
        return { skPath: skPath, contextClause: contextClause };
    }
    function getQuery(startTime, endTime, pathElements) {
        var _a = getContextClause(pathElements), skPath = _a.skPath, contextClause = _a.contextClause;
        var measurements = skPath ? "/" + skPath + ".*/" : '/.*/';
        var whereClause = "time > '" + startTime.toISOString() + "' and time <= '" + endTime.toISOString() + "' " + contextClause;
        var query = measurements + "\n        where " + whereClause + " group by context order by time desc limit 1";
        return { query: query, skPath: skPath, whereClause: whereClause };
    }
    function getHistory(startTime, endTime, pathElements, cb) {
        var _a = getQuery(startTime, endTime, pathElements), query = _a.query, skPath = _a.skPath, whereClause = _a.whereClause;
        app.debug('history query: %s', query);
        clientP.then(function (client) {
            client.query("select * from " + query)
                .then(function (result) {
                var attitude = {};
                var deltas = result.groupRows.map(function (row) {
                    var _a;
                    var path = row.name;
                    var value = row.rows[0].value;
                    var _b = row.rows[0], source = _b.source, stringValue = _b.stringValue, jsonValue = _b.jsonValue, boolValue = _b.boolValue;
                    var context;
                    if (row.tags && row.tags.context) {
                        context = row.tags.context;
                    }
                    else {
                        context = 'vessels.' + app.selfId;
                    }
                    var ts = row.rows[0].time.toISOString();
                    if (path.startsWith('navigation.attitude')) {
                        var parts = path.split('.');
                        attitude[parts[parts.length - 1]] = value;
                        attitude.timestamp = ts;
                        attitude.$source = source;
                        attitude.context = context;
                        return null;
                    }
                    else {
                        if (jsonValue != null) {
                            value = JSON.parse(jsonValue);
                        }
                        else if (stringValue != null) {
                            value = stringValue;
                        }
                        else if (boolValue != null) {
                            value = boolValue;
                        }
                        if (path.indexOf('.') == -1) {
                            value = (_a = {}, _a[path] = value, _a);
                            path = '';
                        }
                        return {
                            context: context,
                            updates: [{
                                    timestamp: ts,
                                    $source: source,
                                    values: [{
                                            path: path,
                                            value: value
                                        }]
                                }]
                        };
                    }
                }).filter(function (d) { return d != null; });
                if (attitude.timestamp) {
                    var ts = attitude.timestamp;
                    var $source = attitude.$source;
                    var context = attitude.context;
                    delete attitude.timestamp;
                    delete attitude.$source;
                    delete attitude.context;
                    deltas.push({
                        context: context,
                        updates: [{
                                timestamp: ts,
                                $source: $source,
                                values: [{
                                        path: 'navigation.attitude',
                                        value: attitude
                                    }]
                            }]
                    });
                }
                //this is for backwards compatibility
                if (skPath == null || skPath === 'navigation' || skPath === 'navigation.position') {
                    var posQuery = "select value from \"navigation.position\" where " + whereClause + " order by time desc limit 1";
                    client.query(posQuery).then(function (result) {
                        if (result.length > 0) {
                            var pos = JSON.parse(result[0].value);
                            deltas.push({
                                context: 'vessels.' + app.selfId,
                                updates: [{
                                        timestamp: result[0].time.toISOString(),
                                        values: [{
                                                path: 'navigation.position',
                                                value: {
                                                    latitude: pos[1],
                                                    longitude: pos[0]
                                                }
                                            }]
                                    }]
                            });
                        }
                        cb(deltas);
                    });
                }
                else {
                    cb(deltas);
                }
            }).catch(function (err) {
                console.error(err);
            });
        }).catch(function (err) {
            console.error(err);
        });
    }
    var plugin = {
        id: 'signalk-to-influxdb',
        name: 'InfluxDb writer',
        description: 'Signal K server plugin that writes self values to InfluxDb',
        schema: {
            type: 'object',
            required: ['host', 'port', 'database'],
            properties: {
                protocol: {
                    type: 'string',
                    enum: [
                        'http',
                        'https'
                    ],
                    default: 'http'
                },
                host: {
                    type: 'string',
                    title: 'Host',
                    default: 'localhost'
                },
                port: {
                    type: 'number',
                    title: 'Port',
                    default: 8086
                },
                username: {
                    type: 'string',
                    title: 'Username (not required if InfluxDb is not authenticated)'
                },
                password: {
                    type: 'string',
                    title: 'Password'
                },
                database: {
                    type: 'string',
                    title: 'Database'
                },
                batchWriteInterval: {
                    type: 'number',
                    title: "Batch writes interval (in seconds, 0 means don't batch)",
                    default: 10,
                },
                resolution: {
                    type: 'number',
                    title: 'Resolution (ms)',
                    default: 200
                },
                recordTrack: {
                    type: "boolean",
                    title: "Record Track",
                    description: "When enabled the vessels position will be stored",
                    default: false
                },
                separateLatLon: {
                    type: "boolean",
                    title: "Latitude and Longitude as separate measurements to Influxdb",
                    description: "Enable location data to be used in various ways e.g. in Grafana (mapping, functions, ...)",
                    default: false
                },
                overrideTimeWithNow: {
                    type: "boolean",
                    title: "Override time with current timestamp",
                    description: "By default the timestamp in the incoming data is used. Check this if you want log playback to simulate getting new data",
                    default: false
                },
                storeOthers: {
                    type: "boolean",
                    title: "Record Others",
                    description: "When enabled data from other vessels, atons and sar aircraft will be stored ",
                    default: false
                },
                blackOrWhite: {
                    type: 'string',
                    title: 'Type of List',
                    description: 'With a blacklist, all numeric values except the ones in the list below will be stored in InfluxDB. With a whitelist, only the values in the list below will be stored.',
                    default: 'Black',
                    enum: ['White', 'Black']
                },
                blackOrWhitelist: {
                    title: 'SignalK Paths',
                    description: 'A list of SignalK paths to be exluded or included based on selection above',
                    type: 'array',
                    items: {
                        type: 'string',
                        title: 'Path'
                    }
                }
            }
        },
        start: function (options) {
            started = true;
            var retryTimeout = 1000;
            function connectToInflux() {
                if (!started) {
                    clientP = Promise.reject('Not started');
                    //Ignore the rejection, otherwise Node complains
                    clientP.catch(function () { });
                    return;
                }
                clientP = influxClientP(options);
                clientP.catch(function (err) {
                    console.error("Error connecting to InfluxDb, retrying in " + retryTimeout + " ms");
                    setTimeout(function () {
                        connectToInflux();
                    }, retryTimeout);
                    retryTimeout *= retryTimeout > 30 * 1000 ? 1 : 2;
                });
            }
            connectToInflux();
            if (app.registerHistoryProvider) {
                app.registerHistoryProvider(plugin);
            }
            if (typeof options.blackOrWhitelist !== 'undefined' &&
                typeof options.blackOrWhite !== 'undefined' &&
                options.blackOrWhitelist.length > 0) {
                var obj = {};
                options.blackOrWhitelist.forEach(function (element) {
                    obj[element] = true;
                });
                if (options.blackOrWhite == 'White') {
                    shouldStore = function (path) {
                        return typeof obj[path] !== 'undefined';
                    };
                }
                else {
                    shouldStore = function (path) {
                        return typeof obj[path] === 'undefined';
                    };
                }
            }
            var deltaToPoints = deltaToPointsConverter(selfContext, options.recordTrack, options.separateLatLon, shouldStore, options.resolution || 200, options.storeOthers, !options.overrideTimeWithNow);
            var accumulatedPoints = [];
            var lastWriteTime = Date.now();
            var batchWriteInterval = (typeof options.batchWriteInterval === 'undefined' ? 10 : options.batchWriteInterval) * 1000;
            var writeFailureCount = 0;
            var writeRetryTimeout = null;
            var MAX_ACCUMULATED_POINTS = 10000;
            var handleDelta = function (delta) {
                var points = deltaToPoints(delta);
                if (points.length > 0) {
                    accumulatedPoints = accumulatedPoints.concat(points);
                    if (accumulatedPoints.length > MAX_ACCUMULATED_POINTS) {
                        logError("signalk-to-influxdb: Dropping old points, buffer exceeded " + MAX_ACCUMULATED_POINTS + "!");
                        accumulatedPoints = accumulatedPoints.slice(-MAX_ACCUMULATED_POINTS);
                    }
                    var now = Date.now();
                    if (batchWriteInterval == 0 || now - lastWriteTime > batchWriteInterval) {
                        lastWriteTime = now;
                        var tryWrite_1 = function () {
                            clientP
                                .then(function (client) {
                                var thePoints = accumulatedPoints;
                                return client.writePoints(thePoints)
                                    .then(function () {
                                    accumulatedPoints = [];
                                    writeFailureCount = 0;
                                    if (writeRetryTimeout) {
                                        clearTimeout(writeRetryTimeout);
                                        writeRetryTimeout = null;
                                    }
                                })
                                    .catch(function (error) {
                                    logError(error);
                                    writeFailureCount++;
                                    var retryDelay = Math.min(120000, 2000 * Math.pow(2, writeFailureCount));
                                    if (!writeRetryTimeout) {
                                        writeRetryTimeout = setTimeout(tryWrite_1, retryDelay);
                                    }
                                });
                            })
                                .catch(function (error) {
                                logError(error);
                                writeFailureCount++;
                                var retryDelay = Math.min(120000, 2000 * Math.pow(2, writeFailureCount));
                                if (!writeRetryTimeout) {
                                    writeRetryTimeout = setTimeout(tryWrite_1, retryDelay);
                                }
                            });
                        };
                        tryWrite_1();
                    }
                }
            };
            app.signalk.on('delta', handleDelta);
            var pruneInterval = setInterval(function () {
                pruneTimestamps(1000 * options.resolution);
            }, 100 * options.resolution);
            unsubscribes.push(function () {
                app.signalk.removeListener('delta', handleDelta);
                clearInterval(pruneInterval);
            });
            HistoryAPI_1.registerHistoryApiRoute(app, clientP, app.selfId, app.debug);
        },
        stop: function () {
            unsubscribes.forEach(function (f) { return f(); });
            started = false;
        },
        signalKApiRoutes: function (router) {
            var trackHandler = function (req, res, next) {
                if (typeof clientP === 'undefined') {
                    console.error('signalk-to-influxdb plugin not enabled, http track interface not available');
                    next();
                    return;
                }
                var endTime = req.query.timespanOffset ? endTimeExpression(req.query.timespan, req.query.timespanOffset) : null;
                var startTime = req.query.timespanOffset ? offset(req.query.timespan, req.query.timespanOffset) : sanitize(req.query.timespan || '1h');
                var query = "\n        select first(value) as \"position\", first(jsonValue) as \"jsonPosition\"\n        from \"navigation.position\"\n        where context = '" + selfContext + "'\n        and time >= now() - " + startTime + " " + (endTime ? ' and time <= ' + endTime + ' ' : '') + "\n        group by time(" + sanitize(req.query.resolution || '1m') + ")";
                clientP.then(function (client) {
                    client.query(query)
                        .then(function (result) {
                        res.type('application/vnd.geo+json');
                        res.json(toMultilineString(result));
                    });
                }).catch(function (err) {
                    console.error(err.message + ' ' + query);
                    res.status(500).send(err.message + ' ' + query);
                });
            };
            router.get('/self/track', trackHandler);
            router.get('/vessels/self/track', trackHandler);
            router.get('/vessels/' + app.selfId + '/track', trackHandler);
            return router;
        },
        historyStreamers: {},
        streamHistory: function (cookie, options, onDelta) {
            var playbackRate = options.playbackRate || 1;
            var startTime = options.startTime;
            app.debug("starting streaming: " + startTime + " " + playbackRate + " ");
            var pathElements = getPathFromOptions(options);
            plugin.historyStreamers[cookie] = setInterval(function () {
                var endTime = new Date(startTime.getTime() + (1000 * playbackRate));
                getHistory(startTime, endTime, pathElements, function (deltas) {
                    app.debug("sending " + deltas.length + " deltas");
                    deltas.forEach(onDelta);
                });
                startTime = endTime;
            }, 1000);
            return function () {
                app.debug("stop streaming: " + cookie);
                clearInterval(plugin.historyStreamers[cookie]);
                delete plugin.historyStreamers[cookie];
            };
        },
        hasAnyData: function (options, cb) {
            var pathElements = getPathFromOptions(options);
            var endTime = new Date(options.startTime.getTime() + (1000 * 10));
            var _a = getQuery(options.startTime, endTime, pathElements), query = _a.query, skPath = _a.skPath;
            clientP.then(function (client) {
                client.query("select count(*) from " + query)
                    .then(function (result) {
                    cb(result.length > 0);
                }).catch(function (err) {
                    console.error(err);
                    cb(false);
                });
            }).catch(function (err) {
                console.error(err);
                cb(false);
            });
        },
        getHistory: function (date, pathElements, cb) {
            var startTime = new Date(date.getTime() - (1000 * 60 * 5));
            getHistory(startTime, date, pathElements, function (deltas) {
                cb(deltas);
            });
        },
    };
    return plugin;
};
var influxDurationKeys = {
    s: 's',
    m: 'm',
    h: 'h',
    d: 'd',
    w: 'w'
};
function sanitize(influxTime) {
    return (Number(influxTime.substring(0, influxTime.length - 1)) +
        influxDurationKeys[influxTime.substring(influxTime.length - 1, influxTime.length)]);
}
function getPathFromOptions(options) {
    if (options.subscribe && options.subscribe === 'self') {
        return ['vessels', 'self'];
    }
    else {
        return null;
    }
}
// offset an influxTime value by offsetTime
function offset(influxTime, offsetTime) {
    var tKey = influxTime.slice(-1);
    if (isNaN(offsetTime)) {
        return sanitize(influxTime);
    }
    var oVal = Number(influxTime.substring(0, influxTime.length - 1)) + Math.abs(Number(offsetTime));
    return sanitize(oVal + influxDurationKeys[tKey]);
}
function endTimeExpression(timespanValue, offsetValue) {
    if (isNaN(offsetValue)) {
        return null;
    }
    else {
        var tKey = timespanValue.slice(-1); // timespan Y value
        return "now() - " + (Math.abs(Number(offsetValue)) + influxDurationKeys[tKey]);
    }
}
