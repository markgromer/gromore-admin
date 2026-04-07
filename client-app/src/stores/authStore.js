import { create } from 'zustand'
import { api } from '../lib/api'

export const useAuthStore = create((set) => ({
  user: null,
  brand: null,
  loading: true,

  init: async () => {
    try {
      const data = await api.get('/api/me')
      set({ user: data.user, brand: data.brand, loading: false })
    } catch {
      set({ user: null, brand: null, loading: false })
    }
  },

  login: async (email, password) => {
    const res = await api.post('/api/login', { email, password })
    if (res.ok) {
      set({ user: res.user, brand: res.brand })
      return { ok: true }
    }
    return { ok: false, error: res.error }
  },

  logout: async () => {
    await api.post('/api/logout', {})
    set({ user: null, brand: null })
    window.location.href = '/client/app/login'
  },
}))
