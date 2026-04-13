import { NavLink, useLocation } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  LayoutDashboard, Megaphone, Users, ClipboardCheck,
  UserPlus, PenTool, Palette, FileText, CalendarClock,
  Zap, GraduationCap, Building2, Globe, Swords, Target,
  Link2, MessageSquareMore, HelpCircle, Sun, Moon,
  ChevronDown, LogOut, X
} from 'lucide-react'
import { useThemeStore } from '../../stores/themeStore'
import { useAuthStore } from '../../stores/authStore'
import logoSrc from '../../assets/WARREN_TRANSPARENT_LOGO.svg'
import styles from './Sidebar.module.css'

const MotionDiv = motion.div
const MotionUl = motion.ul

const navSections = [
  {
    id: 'primary',
    items: [
      { to: '/', icon: LayoutDashboard, label: 'Overview' },
      { to: '/campaigns', icon: Megaphone, label: 'Campaigns' },
      { to: '/crm', icon: Users, label: 'CRM' },
      { to: '/actions', icon: ClipboardCheck, label: 'Missions' },
      { to: '/hiring', icon: UserPlus, label: 'Hiring Hub' },
    ],
  },
  {
    id: 'create',
    label: 'Create',
    items: [
      { to: '/ad-builder', icon: PenTool, label: 'Ad Builder' },
      { to: '/creative', icon: Palette, label: 'Creative' },
      { to: '/blog', icon: FileText, label: 'Blog' },
      { to: '/post-scheduler', icon: CalendarClock, label: 'Post Scheduler' },
      { to: '/quick-launch', icon: Zap, label: 'Quick Launch' },
      { to: '/coaching', icon: GraduationCap, label: 'Coaching' },
    ],
  },
  {
    id: 'business',
    label: 'Business',
    items: [
      { to: '/my-business', icon: Building2, label: 'My Business' },
      { to: '/google-business-profile', icon: Globe, label: 'Google Profile' },
      { to: '/competitors', icon: Swords, label: 'Competitor Intel' },
      { to: '/kpis', icon: Target, label: 'KPIs' },
      { to: '/heatmap', icon: Target, label: 'Rank Heatmap' },
    ],
  },
  {
    id: 'utility',
    items: [
      { to: '/settings', icon: Link2, label: 'Connections' },
      { to: '/feedback', icon: MessageSquareMore, label: 'Feedback' },
      { to: '/help', icon: HelpCircle, label: 'Help' },
    ],
  },
]

export default function Sidebar({ mobileOpen, onClose }) {
  const { theme, toggle } = useThemeStore()
  const { user, brand, logout } = useAuthStore()
  const location = useLocation()

  const [collapsed, setCollapsed] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem('gromore-nav-sections') || '{}')
    } catch { return {} }
  })
  const [pendingPath, setPendingPath] = useState('')

  const toggleSection = (id) => {
    setCollapsed(prev => {
      const next = { ...prev, [id]: !prev[id] }
      localStorage.setItem('gromore-nav-sections', JSON.stringify(next))
      return next
    })
  }

  // Close mobile sidebar on navigation
  useEffect(() => {
    if (mobileOpen) onClose()
  }, [location.pathname, mobileOpen, onClose])

  return (
    <>
      {/* Mobile backdrop */}
      <AnimatePresence>
        {mobileOpen && (
          <MotionDiv
            className={styles.backdrop}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />
        )}
      </AnimatePresence>

      <nav className={`${styles.sidebar} ${mobileOpen ? styles.open : ''}`}>
        {/* Brand */}
        <div className={styles.brand}>
          <img src={logoSrc} alt="GroMore" className={styles.brandLogo} />
          <span className={styles.brandText}>{brand?.display_name || 'Dashboard'}</span>
          <button className={styles.mobileClose} onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        {/* Nav */}
        <div className={styles.nav}>
          {navSections.map((section) => (
            <div key={section.id} className={styles.section}>
              {section.label && (
                <button
                  className={`${styles.sectionLabel} ${collapsed[section.id] ? styles.collapsed : ''}`}
                  onClick={() => toggleSection(section.id)}
                >
                  <span>{section.label}</span>
                  <ChevronDown size={14} className={styles.chevron} />
                </button>
              )}
              <AnimatePresence initial={false}>
                {!collapsed[section.id] && (
                  <MotionUl
                    className={styles.items}
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.2, ease: [0.4, 0, 0.2, 1] }}
                  >
                    {section.items.map((item) => {
                      const NavIcon = item.icon
                      return (
                        <li key={item.to}>
                          <NavLink
                            to={item.to}
                            end={item.to === '/'}
                            className={({ isActive }) =>
                              `${styles.link} ${isActive ? styles.active : ''} ${pendingPath === item.to && !isActive ? styles.pending : ''}`
                            }
                            onClick={() => {
                              if (location.pathname !== item.to) {
                                setPendingPath(item.to)
                              }
                            }}
                          >
                            <NavIcon size={18} strokeWidth={1.8} />
                            <span>{item.label}</span>
                          </NavLink>
                        </li>
                      )
                    })}
                  </MotionUl>
                )}
              </AnimatePresence>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className={styles.footer}>
          <button className={styles.themeToggle} onClick={toggle}>
            {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
            <span>{theme === 'dark' ? 'Light Mode' : 'Dark Mode'}</span>
          </button>
          <div className={styles.user}>
            <span className={styles.userName}>{user?.display_name || 'User'}</span>
            <button className={styles.logoutBtn} onClick={logout}>
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </nav>
    </>
  )
}
