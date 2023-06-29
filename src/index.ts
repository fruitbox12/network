import EventEmitter from 'events'
import { Duplex } from 'streamx'
import DHT, { Server, Socket, KeyPair } from '@hyperswarm/dht'
import { toBase32 } from './util.js'

// globals
// =

let node: DHT | undefined = undefined
const activeNodes: Node[] = []

// exported api
// =

export * as http from './http.js'

export async function setup (opts?: {bootstrap: string[]}): Promise<void> {
  if (node) throw new Error(' DHT already active')
  node = new DHT(opts)
}

export async function destroy () {
  if (node) {
    await node.destroy()
    node = undefined
  }
}

export function createKeypair (seed?: Buffer) {
  return DHT.keyPair(seed)
}

export interface ShadowNodeProtocolHandler {
  (stream: Duplex, socket: ShadowSocket): void|Promise<void>
}

export class Node extends EventEmitter {
  sockets: Map<string, ShadowSocket[]> = new Map()
  hyperswarmServer: Server|undefined
  protocolHandlers: Map<string, ShadowNodeProtocolHandler> = new Map()
  constructor (public keyPair: KeyPair) {
    super()
  }

  get isListening () {
    return !!this.hyperswarmServer
  }

  get publicKeyB32 () {
    return toBase32(this.keyPair.publicKey)
  }

  get httpHostname () {
    return `${this.publicKeyB32}.atek.app`
  }

  get httpUrl () {
    return `http://${this.httpHostname}`
  }

  async connect (remotePublicKey: Buffer, protocol?: string): Promise<ShadowSocket> {
    const shadowSocket = new shadowSocket({
      remotePublicKey,
      keyPair: this.keyPair,
      client: true,
      protocol
    })
    await initOutboundSocket(this, shadowSocket)
    this.addSocket(shadowSocket)
    shadowSocket.hyperswarmSocket?.on('close', () => {
      this.removeSocket(shadowSocket)
    })
    return shadowSocket
  }

  async listen (): Promise<void> {
    await initListener(this)
  }

  async close () {
    for (const socks of this.sockets.values()) {
      for (const sock of socks) {
        sock.close()
      }
    }
    this.sockets = new Map()

    await this.hyperswarmServer?.close()
    this.hyperswarmServer = undefined

    const i = findIndex(this.keyPair)
    if (i !== -1) activeNodes.splice(i, 1)
  }

  setProtocolHandler (protocol: string|ShadowNodeProtocolHandler, handler?: ShadowNodeProtocolHandler) {
    if (typeof protocol === 'string' && handler) {
      protocol = '*' // TODO: temporary hack until https://github.com/hyperswarm/dht/issues/57 lands
      this.protocolHandlers.set(protocol, handler)
    } else if (typeof protocol !== 'string') {
      this.protocolHandlers.set('*', protocol)
    }
  }

  removeProtocolHandler (protocol = '*') {
    this.protocolHandlers.delete(protocol)
  }

  getSocket (remotePublicKey: Buffer) {
    return this.getAllSockets(remotePublicKey)[0]
  }

  getAllSockets (remotePublicKey: Buffer) {
    const remotePublicKeyB32 = toBase32(remotePublicKey)
    return this.sockets.get(remotePublicKeyB32) || []
  }

  addSocket (shadowSocket: ShadowSocket) {
    const remotePublicKeyB32 = toBase32(shadowSocket.remotePublicKey)
    const arr = this.sockets.get(remotePublicKeyB32) || []
    arr.push(shadowSocket)
    this.sockets.set(remotePublicKeyB32, arr)
  }

  removeSocket (shadowSocket: ShadowSocket) {
    const remotePublicKeyB32 = toBase32(shadowSocket.remotePublicKey)
    let arr = this.sockets.get(remotePublicKeyB32) || []
    arr = arr.filter(s => s !== shadowSocket)
    this.sockets.set(remotePublicKeyB32, arr)
  }
}

export class ShadowSocket extends EventEmitter {
  remotePublicKey: Buffer
  keyPair: KeyPair
  client: boolean
  server: boolean
  protocol: string|undefined
  hyperswarmSocket: Socket|undefined
  // muxer: Mplex|undefined
  constructor (opts: {remotePublicKey: Buffer, keyPair: KeyPair, client?: boolean, server?: boolean, protocol?: string}) {
    super()
    this.remotePublicKey = opts.remotePublicKey
    this.keyPair = opts.keyPair
    this.client = opts.client || false
    this.server = opts.server || false
    this.protocol = opts.protocol || '*'
  }

  get remotePublicKeyB32 () {
    return toBase32(this.remotePublicKey)
  }

  get stream () {
    return this.hyperswarmSocket
  }

  async close () {
    await this.hyperswarmSocket?.close()
    // this.muxer = undefined
    this.hyperswarmSocket = undefined
  }
  
  // async select (protocols: string[]): Promise<{protocol: string, stream: Duplex}> {
  //   if (!this.muxer) throw new Error('Error: this connection is not active')
  //   const muxedStream = this.muxer.newStream()
  //   const mss = new MSS.Dialer(muxedStream)
  //   const res = await mss.select(protocols)
  //   return {
  //     protocol: res.protocol,
  //     stream: toDuplex(res.stream)
  //   }
  // }
}

// internal methods
// =

function findIndex (keyPair: KeyPair) {
  return activeNodes.findIndex(s => Buffer.compare(s.keyPair.publicKey, keyPair.publicKey) === 0)
}

async function initListener (shadowNode: Node) {
  if (!node) throw new Error('Cannot listen: Hyperswarm not active')
  if (shadowNode.hyperswarmServer) return
  
  activeNodes.push(shadowNode)

  shadowNode.hyperswarmServer = node.createServer((hyperswarmSocket: Socket) => {
    const shadowSocket = new shadowSocket({
      remotePublicKey: hyperswarmSocket.remotePublicKey,
      keyPair: shadowNode.keyPair,
      server: true
    })
    shadowSocket.hyperswarmSocket = hyperswarmSocket
    initInboundSocket(shadowNode, shadowSocket)
    shadowNode.addSocket(shadowSocket)
    shadowNode.emit('connection', shadowSocket)
    hyperswarmSocket.once('close', () => {
      shadowNode.removeSocket(shadowSocket)
    })
  })

  await shadowNode.hyperswarmServer.listen(shadowNode.keyPair)
}

function initInboundSocket (shadowNode: Node, shadowSocket: ShadowSocket) {
  if (!shadowSocket.hyperswarmSocket) throw new Error('Hyperswarm Socket not initialized')
  initSocket(shadowNode, shadowSocket)

  const protocol = '*' // TODO: waiting on handshake userData buffer (https://github.com/hyperswarm/dht/issues/57)
  const handler = shadowNode.protocolHandlers.get(protocol) || shadowNode.protocolHandlers.get('*')
  if (handler) {
    handler(shadowSocket.hyperswarmSocket, shadowSocket)
  } else {
    shadowNode.emit('select', {protocol, stream: shadowSocket.hyperswarmSocket}, shadowSocket)
    shadowSocket.emit('select', {protocol, stream: shadowSocket.hyperswarmSocket})
  }
}

async function initOutboundSocket (shadowNode: Node, shadowSocket: ShadowSocket) {
  if (!node) throw new Error('Cannot connect: Hyperswarm DHT not active')
  if (shadowSocket.hyperswarmSocket) return

  const hyperswarmSocket = shadowSocket.hyperswarmSocket = node.connect(shadowSocket.remotePublicKey, {keyPair: shadowSocket.keyPair})
  await new Promise((resolve, reject) => {
    hyperswarmSocket.once('open', () => resolve(undefined))
    hyperswarmSocket.once('close', () => resolve(undefined))
    hyperswarmSocket.once('error', reject)
  })

  initSocket(shadowNode, shadowSocket)
}

function initSocket (shadowNode: Node, shadowSocket: ShadowSocket) {
  if (!shadowSocket.hyperswarmSocket) throw new Error('Hyperswarm Socket not initialized')

  // HACK
  // there are some nodejs stream features that are missing from streamx's Duplex
  // we're going to see if it's a problem to just noop them
  // cork and uncork, for instance, are optimizations that we can probably live without
  // -prf
  // @ts-ignore Duck-typing to match what is expected
  shadowSocket.hyperswarmSocket.cork = noop
  // @ts-ignore Duck-typing to match what is expected
  shadowSocket.hyperswarmSocket.uncork = noop
  // @ts-ignore Duck-typing to match what is expected
  shadowSocket.hyperswarmSocket.setTimeout = noop
  
  // HACK
  // this is a specific issue that's waiting on https://github.com/streamxorg/streamx/pull/46
  // -prf
  // @ts-ignore Monkey patchin'
  shadowSocket.hyperswarmSocket._ended = false
  const _end = shadowSocket.hyperswarmSocket.end
  shadowSocket.hyperswarmSocket.end = function (data: any) {
    _end.call(this, data)
    // @ts-ignore Monkey patchin'
    this._ended = true
  }
  Object.defineProperty(shadowSocket.hyperswarmSocket, 'writable', {
    get() {
      return !this._ended && this._writableState !== null ? true : undefined
    }
  })

  shadowSocket.hyperswarmSocket.once('close', () => {
    shadowSocket.emit('close')
  })
}

function noop () {}
