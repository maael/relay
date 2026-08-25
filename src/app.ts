import express from 'express'
import http from 'http'
import { Server } from 'socket.io'
import chalk, { Chalk } from 'chalk'
import eventHandlers from './handlers'
import { safelyReviveJsonString, replacer } from './util'
import spotifySetup from './handlers/spotify/setup'

const app = express()
const server = http.createServer(app)
const port = process.env.PORT || 3003

const io = new Server(server, {
  cors: {
    origin: true,
    methods: ['GET'],
  },
  transports: ['websocket', 'polling'],
  // Compress larger state updates while avoiding compression overhead for small events.
  perMessageDeflate: { threshold: 1024 },
  // Reduce keepalive traffic for long-lived, mostly idle websocket connections.
  pingInterval: 60_000,
})

io.on('connection', (socket) => {
  console.warn(chalk.yellow('⚠️  Using the default namespace is not supported'))
  socket.emit('error', JSON.stringify({ error: 'Using the default namespace is not supported' }))
  socket.disconnect()
})

const namespaces = [
  { name: '/party-party', chalk: chalk.red, sendUpdates: true, updateTime: 500 },
  { name: '/giveaway-o-tron', chalk: chalk.hex('#6441a5'), dontWarnAny: true },
  { name: '/spotify', chalk: chalk.green, setup: spotifySetup, sendUpdates: true, updateTime: 1_000 },
  { name: '/patreon-herald', chalk: chalk.hex('#f96854') },
  { name: '/uk-county-map', chalk: chalk.hex('#384e1d') },
]

const namespaceRoomStores: { [k: string]: any } = {}
const lastSentRoomStates: { [k: string]: string } = {}

function getRoomStateKey(namespace: string, room: string) {
  return `${namespace}/${room}`
}

function serializeRoomState(namespace: string, room: string) {
  return JSON.stringify(namespaceRoomStores[getRoomStateKey(namespace, room)], replacer)
}

function createNamespacedLogger(ns: string, color: Chalk) {
  const cleanNs = ns.slice(1)
  return {
    warn: function warn(...args: any) {
      console.warn(color(`[${cleanNs}]`), '⚠️ ', ...args)
    },
    error: function error(...args: any) {
      console.error(color(`[${cleanNs}]`), '☠️ ', ...args)
    },
    info: function info(...args: any) {
      console.info(color(`[${cleanNs}]`), '📝', ...args)
    },
  }
}

for (const ns of namespaces) {
  const { name: nsName, chalk: c, ...remaining } = ns
  const logger = createNamespacedLogger(nsName, c)
  const handlers = eventHandlers[nsName] || {}
  logger.info('Starting...')
  logger.info('Config:', remaining)
  logger.info(`Found ${Object.keys(handlers).length} handlers`)
  io.of(nsName).on('connection', (socket) => {
    const channelRoom =
      socket.handshake.query?.room?.toString() || socket.handshake.query?.channel?.toString() || 'default'
    let store = namespaceRoomStores[`${nsName}/${channelRoom}`]
    if (!store) {
      store = {}
      namespaceRoomStores[`${nsName}/${channelRoom}`] = store
    }
    logger.info('[connection]', { ns: nsName, channel: channelRoom })
    handlers.connection?.call(undefined, { socket, store, logger, room: channelRoom })
    if (channelRoom) {
      socket.join(`${channelRoom}`)
    }

    // New clients need a complete snapshot, but existing clients should not receive
    // the same state again merely because another client connected.
    if (ns.sendUpdates) {
      const state = serializeRoomState(nsName, channelRoom)
      lastSentRoomStates[getRoomStateKey(nsName, channelRoom)] = state
      socket.emit('event', { type: 'update', data: state })
    }
    socket.on('event', async (msg: { type?: string }) => {
      let msgObject = msg
      if (typeof msg === 'string') {
        msgObject = safelyReviveJsonString(logger, msg)
      }
      if (handlers.any) {
        handlers.any({ socket, store, logger, msg: msgObject, room: channelRoom })
      }

      const type = msgObject?.type
      if (handlers[type]) {
        handlers[type]?.call(undefined, { socket, store, logger, msg: msgObject, room: channelRoom })
      } else if (!ns.dontWarnAny) {
        logger.warn('[warn][missed-event]', { msg })
      }
    })
    socket.on('disconnect', () => {
      logger.info('[disconnect]')
      handlers.disconnect?.call(undefined, { socket, store, logger, room: channelRoom })
    })
  })

  if (ns.sendUpdates) {
    setInterval(() => {
      const rooms = [...io.of(ns.name).adapter.rooms.keys()]
      const sockets = [...io.of(ns.name).adapter.sids.keys()]
      const nonSocketRooms = rooms.filter((id) => !sockets.includes(id))
      for (const room of nonSocketRooms) {
        const roomStateKey = getRoomStateKey(ns.name, room)
        const state = serializeRoomState(ns.name, room)

        // State is retained between updates. Do not retransmit an identical full
        // snapshot on every interval.
        if (lastSentRoomStates[roomStateKey] === state) {
          continue
        }

        lastSentRoomStates[roomStateKey] = state
        io.of(ns.name).to(room).emit('event', { type: 'update', data: state })
      }
    }, ns.updateTime)
  }

  if (ns.setup) {
    logger.info('[setup:start]')
    ns.setup(ns, namespaceRoomStores)
    logger.info('[setup:end]')
  }
}

app.get('/', (_req, res) => {
  res.send(`<h1>${io.engine.clientsCount} client${io.engine.clientsCount === 1 ? '' : 's'}</h1>`)
})

server.listen(port, () => {
  console.log(`http://localhost:${port}`)
})
