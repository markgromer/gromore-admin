const API_BASE = '/client'

async function request(path, options = {}) {
  const url = path.startsWith('http') ? path : `${API_BASE}${path}`

  const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content')

  const headers = {
    ...options.headers,
  }

  if (csrfToken && options.method && options.method !== 'GET') {
    headers['X-CSRFToken'] = csrfToken
  }

  if (options.json !== undefined) {
    headers['Content-Type'] = 'application/json'
    options.body = JSON.stringify(options.json)
    delete options.json
  }

  const res = await fetch(url, {
    credentials: 'same-origin',
    ...options,
    headers,
  })

  if (res.status === 401 || res.status === 403) {
    window.location.href = '/client/app/login'
    throw new Error('Unauthorized')
  }

  return res
}

export const api = {
  get: (path) => request(path).then(r => r.json()),

  post: (path, data) => request(path, {
    method: 'POST',
    json: data,
  }).then(r => r.json()),

  postForm: (path, formData) => request(path, {
    method: 'POST',
    body: formData,
  }).then(r => r.json()),

  put: (path, data) => request(path, {
    method: 'PUT',
    json: data,
  }).then(r => r.json()),

  del: (path) => request(path, {
    method: 'DELETE',
  }).then(r => r.json()),

  raw: request,
}
