// Tests for the delta -> InfluxDB points converter.
//
// Run with:  npm run build && npm test
const test = require('node:test')
const assert = require('node:assert/strict')
const { deltaToPointsConverter } = require('../built/skToInflux')

const SELF_CONTEXT = 'vessels.urn:mrn:imo:mmsi:230099999'

let tick = 0
function makeConverter() {
  return deltaToPointsConverter(SELF_CONTEXT, true, true, () => true, 0, false, true)
}

function delta(path, value, source = 'src') {
  // unique timestamps so the per-path resolution throttling never drops a value
  const timestamp = new Date(Date.UTC(2026, 0, 1) + (tick += 10000)).toISOString()
  return {
    context: 'vessels.self',
    updates: [{ $source: source, timestamp, values: [{ path, value }] }]
  }
}

test('null navigation.attitude does not throw and produces no points', () => {
  const convert = makeConverter()
  assert.deepEqual(convert(delta('navigation.attitude', null)), [])
})

test('valid navigation.attitude is split into numeric components', () => {
  const convert = makeConverter()
  const points = convert(delta('navigation.attitude', { pitch: 0.1, roll: null, yaw: 0.3 }))
  assert.deepEqual(points.map(p => p.measurement), [
    'navigation.attitude.pitch',
    'navigation.attitude.yaw'
  ])
})

test('null or incomplete navigation.position does not throw and is skipped', () => {
  const convert = makeConverter()
  assert.deepEqual(convert(delta('navigation.position', null, 'a')), [])
  assert.deepEqual(convert(delta('navigation.position', { latitude: 60 }, 'b')), [])
})

test('a skipped null position does not suppress the next valid one', () => {
  const convert = makeConverter()
  convert(delta('navigation.position', null, 'c'))
  const points = convert(delta('navigation.position', { latitude: 60, longitude: 25 }, 'c'))
  assert.equal(points.length, 2)
  assert.deepEqual(points[1].fields, { lon: 25, lat: 60 })
})

test('null root-level value does not throw', () => {
  const convert = makeConverter()
  assert.deepEqual(convert(delta('', null)), [])
})

test('undefined values are skipped, null values are kept as jsonValue', () => {
  const convert = makeConverter()
  assert.deepEqual(convert(delta('environment.depth.belowKeel', undefined)), [])
  const points = convert(delta('environment.depth.belowTransducer', null))
  assert.deepEqual(points[0].fields, { jsonValue: 'null' })
})

test('a skipped null attitude does not suppress the next valid one', () => {
  const convert = deltaToPointsConverter(SELF_CONTEXT, true, true, () => true, 60000, false, true)
  const first = delta('navigation.attitude', null, 'd')
  const next = delta('navigation.attitude', { pitch: 0.1 }, 'd')
  // same timestamp, well within the 60s resolution
  next.updates[0].timestamp = first.updates[0].timestamp
  assert.deepEqual(convert(first), [])
  assert.equal(convert(next).length, 1)
})

test('non-finite position coordinates are skipped', () => {
  const convert = makeConverter()
  assert.deepEqual(convert(delta('navigation.position', { latitude: Infinity, longitude: 25 }, 'e')), [])
})
