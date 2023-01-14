import fetch from 'node-fetch'

const CLIENT_ID = process.env.SPOTIFY_CLIENT_ID as string
const CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET as string
const REFRESH_TOKEN = process.env.SPOTIFY_REFRESH_TOKEN as string
const IS_DEV = process.env.ENVIRONMENT !== 'production'
const PERSONAL_SECRET = process.env.SPOTIFY_PERSONAL_SECRET as string

async function getRefreshedToken() {
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: REFRESH_TOKEN,
  })
  const result = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    body,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64')}`,
    },
  })
  const json = (await result.json()) as { access_token: string }
  return json.access_token
}

const info = async () => {
  const accessToken = await getRefreshedToken()
  const result = await fetch('https://api.spotify.com/v1/me', {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  })
  const json = (await result.json()) as any
  return { ...json, ok: 1 }
}

const playing = async () => {
  const accessToken = await getRefreshedToken()
  const result = await fetch('https://api.spotify.com/v1/me/player', {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  })
  const text = await result.text()
  if (text) {
    const json = JSON.parse(text)
    return { ...json, isPlaying: true, ok: 1 }
  } else {
    return { isPlaying: false, ok: 1 }
  }
}

function isAllowed(data: any) {
  if (data.secret !== PERSONAL_SECRET) {
    console.warn('[spotify][warn][wrong-secret]', data)
    return false
  }
  return true
}

const pause = async (body: any) => {
  if (!isAllowed(body)) return
  const accessToken = await getRefreshedToken()
  let result
  try {
    result = await fetch('https://api.spotify.com/v1/me/player/pause', {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
      method: 'PUT',
    })
    if (result.status === 403) throw new Error('Paused already')
  } catch {
    result = await fetch('https://api.spotify.com/v1/me/player/play', {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
      method: 'PUT',
    })
  }
  const text = await result.text()
  if (text) {
    const json = JSON.parse(text)
    return { ...json, isPlaying: true, ok: 1 }
  } else {
    return { isPlaying: false, ok: 1 }
  }
}

async function spotifyApi(
  access: 'restricted' | 'open',
  data: any,
  path: string,
  method: 'GET' | 'POST' | 'PUT' = 'GET'
) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let allowedStatus: any = { ok: true }
  if (access === 'restricted') {
    allowedStatus = isAllowed(data)
    if (!allowedStatus.ok) return allowedStatus
  }
  const accessToken = await getRefreshedToken()
  const result = await fetch(`https://api.spotify.com/v1${path}`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
    method,
  })
  let resultData = await result.text()
  let type = 'text'
  if (resultData) {
    resultData = JSON.parse(resultData)
    type = 'json'
  }
  return { data: resultData, type, ok: true }
}

const next = async (body: any) => {
  const { data, ok } = await spotifyApi('restricted', body, '/me/player/next', 'POST')
  if (ok) {
    return { ...data, ok: 1 }
  } else {
    return { ...data, ok: 0 }
  }
}

const previous = async (body: any) => {
  const { data, ok } = await spotifyApi('restricted', body, '/me/player/previous', 'POST')
  if (ok) {
    return { ...data, ok: 1 }
  } else {
    return { ...data, ok: 0 }
  }
}

const actions = {
  info,
  playing,
  pause,
  next,
  previous,
}

export default actions
