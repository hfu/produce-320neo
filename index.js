const config = require('config')
const Queue = require('better-queue')
const { spawnSync, spawn } = require('child_process')
const tilebelt = require('@mapbox/tilebelt')
const fs = require('fs')
const path = require('path')
const TimeFormat = require('hh-mm-ss')
const pretty = require('prettysize')
const modify = require(config.get('modifyPath'))
const winston = require('winston')
const tempy = require('tempy')
const Parser = require('json-text-sequence').parser

winston.configure({
  transports: [new winston.transports.Console()]
})

// configuration constants
const z = config.get('z')
const minx = config.get('minx')
const miny = config.get('miny')
const maxx = config.get('maxx')
const maxy = config.get('maxy')
const planetPath = config.get('planetPath')
const exportConfigPath = config.get('exportConfigPath')
const mbtilesDirPath = config.get('mbtilesDirPath')

const iso = () => {
  return new Date().toISOString()
}

const produce = (z, x, y) => {
  return new Promise((resolve, reject) => {
    const startTime = new Date()
    const bbox = tilebelt.tileToBBOX([x, y, z])
    const tmpPath = `${mbtilesDirPath}/part-${z}-${x}-${y}.mbtiles`
    const dstPath = `${mbtilesDirPath}/${z}-${x}-${y}.mbtiles`
    winston.info(`${iso()}: ${z}-${x}-${y} production started`)

    if (fs.existsSync(dstPath)) {
      winston.info(`${iso()}: ${dstPath} already there.`)
      resolve(null)
    } else {
      const tippecanoe = spawn('tippecanoe', [
        '--no-feature-limit', '--no-tile-size-limit',
        '--force', '--simplification=2', '--no-progress',
        '--minimum-zoom=6', '--maximum-zoom=15', '--base-zoom=15',
        `--clip-bounding-box=${bbox.join(',')}`, '--hilbert',
        `--output=${tmpPath}`
      ], {
        stdio: ['pipe', 'inherit', 'ignore']
      })
      tippecanoe.on('close', () => {
        fs.renameSync(tmpPath, dstPath)
        const s = TimeFormat.fromMs(new Date() - startTime)
        winston.info(`${iso()}: ${z}-${x}-${y} production finished in ${s}`)
        resolve(null)
      })

      let pausing = false
      const jsonTextSequenceParser = new Parser()
      .on('data', (json) => {
        f = modify(json)
        if (f) {
          if (tippecanoe.stdin.write(JSON.stringify(f))) {
          } else {
            osmium.stdout.pause()
            if (!pausing) {
              tippecanoe.stdin.once('drain', () => {
                osmium.stdout.resume()
                pausing = false
              })
              pausing = true
            }
          }
        }
      })
      .on('finish', () => {
        tippecanoe.stdin.end()
      })

      const osmiumExport = spawn('osmium', [
        'export', '--index-type=sparse_file_array',
        `--config=${exportConfigPath}`, 
        '--output-format=geojsonseq', 
        '--input-format=opl',
      ], { 
        stdio: ['pipe', 'pipe', 'inherit']
      })
      osmiumExport.stdout.pipe(jsonTextSequenceParser)

      const osmiumExtract = spawn('osmium', [
        'extract', '--bbox', bbox.join(','),
        '--strategy=smart', '--progress',
        '--output-format=opl',
        planetPath
      ], {
        stdio: ['inherit', 'pipe', 'inherit']
      })
      osmiumExtract.stdout.pipe(osmiumExport.stdin)
      osmiumExtract.stdout.pipe(new fs.createWriteStream(`${z}-${x}-${y}.opl`))
   }
  })
}

const queue = new Queue(async (t, cb) => {
  const [z, x, y] = t
  await produce(z, x, y)
  return cb(null)
}, { concurrent: config.get('concurrent') })

queue.on('task_failed', (taskId, err, stats) => {
  winston.error(err.stack)
})

for (let x = minx; x <= maxx; x++) {
  for (let y = miny; y <= maxy; y++) {
    queue.push([z, x, y])
  }
}
