import { create } from 'zustand'
import { api } from '../lib/api'

export const useDashboardStore = create((set, get) => ({
  data: null,
  loading: false,
  error: null,
  month: new Date().toISOString().slice(0, 7),

  setMonth: (month) => set({ month }),

  fetch: async (opts = {}) => {
    const { month } = get()
    set({ loading: true, error: null })
    try {
      const params = new URLSearchParams({ month })
      if (opts.refresh) params.set('refresh', '1')
      if (opts.sync) params.set('sync', '1')
      const res = await api.get(`/dashboard/data?${params}`)
      if (res.error) {
        set({ error: res.error, loading: false })
      } else {
        set({ data: res.dashboard, loading: false })
      }
    } catch (err) {
      set({ error: err.message, loading: false })
    }
  },
}))
