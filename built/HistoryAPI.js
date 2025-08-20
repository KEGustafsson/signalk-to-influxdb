"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArrays = (this && this.__spreadArrays) || function () {
    for (var s = 0, i = 0, il = arguments.length; i < il; i++) s += arguments[i].length;
    for (var r = Array(s), k = 0, i = 0; i < il; i++)
        for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
            r[k] = a[j];
    return r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerHistoryApiRoute = void 0;
var core_1 = require("@js-joda/core");
function registerHistoryApiRoute(router, influx, selfId, debug) {
    var _this = this;
    router.get("/signalk/v1/history/values", asyncHandler(fromToHandler(function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        return getValues.apply(_this, __spreadArrays([influx, selfId], args));
    }, debug)));
    router.get("/signalk/v1/history/contexts", asyncHandler(fromToHandler(function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        return getContexts.apply(_this, __spreadArrays([influx, selfId], args));
    }, debug)));
    router.get("/signalk/v1/history/paths", asyncHandler(fromToHandler(function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        return getPaths.apply(_this, __spreadArrays([influx, selfId], args));
    }, debug)));
}
exports.registerHistoryApiRoute = registerHistoryApiRoute;
function getContexts(influx, from, to, debug) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, influx
                    .then(function (i) {
                    return i.query('SHOW TAG VALUES FROM "navigation.position" WITH KEY = "context"');
                })
                    .then(function (x) { return x.map(function (x) { return x.value; }); })];
        });
    });
}
function getPaths(influx, from, to, debug, req) {
    return __awaiter(this, void 0, void 0, function () {
        var query;
        return __generator(this, function (_a) {
            query = "SHOW MEASUREMENTS";
            console.log(query);
            return [2 /*return*/, influx
                    .then(function (i) { return i.query(query); })
                    .then(function (d) { return d.map(function (r) { return r.name; }); })];
        });
    });
}
function getValues(influx, selfId, from, to, debug, req) {
    return __awaiter(this, void 0, void 0, function () {
        var timeResolutionSeconds, context, pathExpressions, pathSpecs, queries, x;
        return __generator(this, function (_a) {
            timeResolutionSeconds = req.query.resolution
                ? Number.parseFloat(req.query.resolution)
                : (to.toEpochSecond() - from.toEpochSecond()) / 500;
            context = getContext(req.query.context, selfId);
            debug(context);
            pathExpressions = (req.query.paths || "")
                .replace(/[^0-9a-z\.,\:]/gi, "")
                .split(",");
            pathSpecs = pathExpressions.map(splitPathExpression);
            queries = pathSpecs
                .map(function (_a) {
                var aggregateFunction = _a.aggregateFunction, path = _a.path;
                return "\n      SELECT\n        " + aggregateFunction + " as value\n      FROM\n      \"" + path + "\"\n      WHERE\n        \"context\" = '" + context + "'\n        AND\n        time > '" + from.format(core_1.DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z' and time <= '" + to.format(core_1.DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z'\n      GROUP BY\n        time(" + Number(timeResolutionSeconds * 1000).toFixed(0) + "ms)";
            })
                .map(function (s) { return s.replace(/\n/g, " ").replace(/ +/g, " "); });
            queries.forEach(function (s) { return debug(s); });
            x = Promise.all(queries.map(function (q) { return influx.then(function (i) { return i.query(q); }); }));
            return [2 /*return*/, x.then(function (results) { return ({
                    context: context,
                    values: pathSpecs.map(function (_a) {
                        var path = _a.path, aggregateMethod = _a.aggregateMethod;
                        return ({
                            path: path,
                            method: aggregateMethod,
                            source: null,
                        });
                    }),
                    range: { from: from.toString(), to: to.toString() },
                    data: toDataRows(results.map(function (r) { return r.groups(); }), pathSpecs.map(function (ps) { return ps.extractValue; })),
                }); })];
        });
    });
}
function getContext(contextFromQuery, selfId) {
    if (!contextFromQuery ||
        contextFromQuery === "vessels.self" ||
        contextFromQuery === "self") {
        return "vessels." + selfId;
    }
    return contextFromQuery.replace(/ /gi, "");
}
var toDataRows = function (dataResults, valueMappers) {
    var resultRows = [];
    dataResults.forEach(function (data, seriesIndex) {
        var series = data[0]; //we always get one result
        var valueMapper = valueMappers[seriesIndex];
        series &&
            series.rows.forEach(function (row, i) {
                if (!resultRows[i]) {
                    resultRows[i] = [];
                }
                resultRows[i][0] = row.time.toNanoISOString();
                resultRows[i][seriesIndex + 1] = valueMapper(row);
            });
    });
    return resultRows;
};
var EXTRACT_POSITION = function (r) {
    if (r.value) {
        var position = JSON.parse(r.value);
        return [position.longitude, position.latitude];
    }
    return null;
};
var EXTRACT_NUMBER = function (r) { return r.value; };
function splitPathExpression(pathExpression) {
    var parts = pathExpression.split(":");
    var aggregateMethod = parts[1] || "average";
    var extractValue = EXTRACT_NUMBER;
    if (parts[0] === "navigation.position") {
        aggregateMethod = "first";
        extractValue = EXTRACT_POSITION;
    }
    return {
        path: parts[0],
        aggregateMethod: aggregateMethod,
        extractValue: extractValue,
        aggregateFunction: functionForAggregate[aggregateMethod] || "MEAN(value)",
    };
}
var functionForAggregate = {
    average: "MEAN(value)",
    min: "MIN(value)",
    max: "MAX(value)",
    first: "FIRST(jsonValue)",
};
function fromToHandler(wrappedHandler, debug) {
    var _this = this;
    return function (req, res) { return __awaiter(_this, void 0, void 0, function () {
        var from, to, _a, _b;
        return __generator(this, function (_c) {
            switch (_c.label) {
                case 0:
                    debug(req.query);
                    from = dateTimeFromQuery(req, "from");
                    to = dateTimeFromQuery(req, "to");
                    debug(from.toString() + "-" + to.toString());
                    _b = (_a = res).json;
                    return [4 /*yield*/, wrappedHandler(from, to, debug, req)];
                case 1:
                    _b.apply(_a, [_c.sent()]);
                    return [2 /*return*/];
            }
        });
    }); };
}
function dateTimeFromQuery(req, paramName) {
    return core_1.ZonedDateTime.parse(req.query[paramName]);
}
function asyncHandler(requestHandler) {
    return function (req2, res2, next) {
        requestHandler(req2, res2).catch(next);
    };
}
