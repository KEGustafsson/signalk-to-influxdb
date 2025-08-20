/*
 * Copyright 2018 Teppo Kurki <teppo.kurki@iki.fi>
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
var Influx = require('influx');
var debug = require('debug')('signalk-to-influxdb');
var getSourceId = require('@signalk/signalk-schema').getSourceId;
var lastUpdates = {};
var lastPositionStored = {};
function addSource(update, tags) {
    if (update['$source']) {
        tags.source = update['$source'];
    }
    else if (update['source']) {
        tags.source = getSourceId(update['source']);
    }
    return tags;
}
module.exports = {
    deltaToPointsConverter: function (selfContext, recordTrack, separateLatLon, shouldStore, resolution, storeOthers, honorDeltaTimestamp) {
        if (honorDeltaTimestamp === void 0) { honorDeltaTimestamp = true; }
        return function (delta) {
            if (delta.context === 'vessels.self') {
                delta.context = selfContext;
            }
            var points = [];
            if (delta.updates && (storeOthers || delta.context === selfContext)) {
                delta.updates.forEach(function (update) {
                    if (update.values) {
                        var date_1 = honorDeltaTimestamp ? new Date(update.timestamp) : new Date();
                        var time_1 = date_1.getTime();
                        var tags_1 = addSource(update, { context: delta.context });
                        update.values.reduce(function (acc, pathValue) {
                            if (pathValue.path === 'navigation.position') {
                                if (recordTrack && shouldStorePositionNow(delta, tags_1.source, time_1)) {
                                    var point = {
                                        measurement: pathValue.path,
                                        tags: tags_1,
                                        timestamp: date_1,
                                        fields: {
                                            jsonValue: JSON.stringify({
                                                longitude: pathValue.value.longitude,
                                                latitude: pathValue.value.latitude
                                            }),
                                        }
                                    };
                                    acc.push(point);
                                    if (separateLatLon) {
                                        var point_1 = {
                                            measurement: pathValue.path,
                                            tags: tags_1,
                                            timestamp: date_1,
                                            fields: {
                                                lon: pathValue.value.longitude,
                                                lat: pathValue.value.latitude
                                            }
                                        };
                                        acc.push(point_1);
                                    }
                                    if (!lastPositionStored[delta.context]) {
                                        lastPositionStored[delta.context] = {};
                                    }
                                    lastPositionStored[delta.context][tags_1.source] = time_1;
                                }
                            }
                            else {
                                var pathAndSource = pathValue.path + "-" + tags_1.source;
                                if (shouldStore(pathValue.path) &&
                                    (pathValue.path == '' || shouldStoreNow(delta, pathAndSource, time_1, resolution))) {
                                    if (!lastUpdates[delta.context]) {
                                        lastUpdates[delta.context] = {};
                                    }
                                    lastUpdates[delta.context][pathAndSource] = time_1;
                                    if (pathValue.path === 'navigation.attitude') {
                                        storeAttitude(date_1, pathValue, tags_1, acc);
                                    }
                                    else {
                                        function addPoint(path, value) {
                                            var _a;
                                            var valueKey = null;
                                            if (typeof value === 'number' &&
                                                !isNaN(value)) {
                                                valueKey = 'value';
                                            }
                                            else if (typeof value === 'string') {
                                                valueKey = 'stringValue';
                                            }
                                            else if (typeof value === 'boolean') {
                                                valueKey = 'boolValue';
                                            }
                                            else {
                                                valueKey = 'jsonValue';
                                                value = JSON.stringify(value);
                                            }
                                            if (valueKey) {
                                                var point = {
                                                    measurement: path,
                                                    timestamp: date_1,
                                                    tags: tags_1,
                                                    fields: (_a = {},
                                                        _a[valueKey] = value,
                                                        _a)
                                                };
                                                acc.push(point);
                                            }
                                        }
                                        if (pathValue.path === '') {
                                            Object.keys(pathValue.value).forEach(function (key) {
                                                addPoint(key, pathValue.value[key]);
                                            });
                                        }
                                        else {
                                            addPoint(pathValue.path, pathValue.value);
                                        }
                                    }
                                }
                            }
                            return acc;
                        }, points);
                    }
                });
            }
            return points;
        };
    },
    influxClientP: function (_a) {
        var protocol = _a.protocol, host = _a.host, port = _a.port, database = _a.database, username = _a.username, password = _a.password;
        debug("Attempting connection to " + host + port + " " + database + " as username " + (username ? username : 'n/a') + " " + (password ? '' : 'no password configured'));
        return new Promise(function (resolve, reject) {
            var influxOptions = {
                host: host,
                port: port,
                protocol: protocol ? protocol : 'http',
                database: database
            };
            if (username) {
                influxOptions.username = username;
                influxOptions.password = password;
            }
            var client = new Influx.InfluxDB(influxOptions);
            client
                .getDatabaseNames()
                .then(function (names) {
                debug('Connected');
                if (names.includes(database)) {
                    resolve(client);
                }
                else {
                    client.createDatabase(database).then(function (result) {
                        debug('Created InfluxDb database ' + database);
                        resolve(client);
                    });
                }
            })
                .catch(function (err) {
                reject(err);
            });
        });
    },
    pruneTimestamps: function (maxAge) {
        clearContextTimestamps(lastUpdates, maxAge);
        clearContextTimestamps(lastPositionStored, maxAge);
    }
};
function clearContextTimestamps(holder, maxAge) {
    Object.keys(holder).forEach(function (context) {
        var newestTimestamp = Object.keys(holder[context]).reduce(function (acc, key) {
            return Math.max(acc, holder[context][key]);
        }, 0);
        if (Date.now() - maxAge > newestTimestamp) {
            delete holder[context];
        }
    });
}
function shouldStorePositionNow(delta, sourceId, time) {
    if (!lastPositionStored[delta.context]) {
        lastPositionStored[delta.context] = {};
    }
    return (!lastPositionStored[delta.context][sourceId]
        || time - lastPositionStored[delta.context][sourceId] > 1000);
}
function shouldStoreNow(delta, pathAndSource, time, resolution) {
    return (!lastUpdates[delta.context] || !lastUpdates[delta.context][pathAndSource] ||
        time - lastUpdates[delta.context][pathAndSource] > resolution);
}
function storeAttitude(date, pathValue, tags, acc) {
    ['pitch', 'roll', 'yaw'].forEach(function (key) {
        if (typeof pathValue.value[key] === 'number' &&
            !isNaN(pathValue.value[key])) {
            acc.push({
                measurement: "navigation.attitude." + key,
                timestamp: date,
                tags: tags,
                fields: {
                    value: pathValue.value[key]
                }
            });
        }
    });
}
