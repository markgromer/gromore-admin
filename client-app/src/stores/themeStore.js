import { create } from 'zustand'

const getInitialTheme = () => {
  if (typeof window === 'undefined') return 'dark'
  const stored = localStorage.getItem('gromore-theme')
  if (stored === 'light' || stored === 'dark') return stored
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export const useThemeStore = create((set) => ({
  theme: getInitialTheme(),
  toggle: () => set((state) => {
    const next = state.theme === 'dark' ? 'light' : 'dark'
    localStorage.setItem('gromore-theme', next)
    document.documentElement.setAttribute('data-theme', next)
    return { theme: next }
  }),
}))

// Apply theme on load
if (typeof window !== 'undefined') {
  document.documentElement.setAttribute('data-theme', getInitialTheme())
}
