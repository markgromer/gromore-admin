import { useState } from 'react'
import { Outlet, useLocation } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import Sidebar from './Sidebar'
import { Menu } from 'lucide-react'
import styles from './Shell.module.css'

const MotionDiv = motion.div

const pageVariants = {
  initial: { opacity: 0, y: 8 },
  animate: { opacity: 1, y: 0 },
  exit: { opacity: 0, y: -8 },
}

export default function Shell() {
  const [mobileOpen, setMobileOpen] = useState(false)
  const location = useLocation()

  return (
    <div className={styles.shell}>
      <Sidebar mobileOpen={mobileOpen} onClose={() => setMobileOpen(false)} />

      <div className={styles.main}>
        {/* Mobile topbar */}
        <header className={styles.topbar}>
          <button className={styles.menuBtn} onClick={() => setMobileOpen(true)}>
            <Menu size={20} />
          </button>
        </header>

        {/* Page content with transitions */}
        <div className={styles.content}>
          <AnimatePresence mode="wait">
            <MotionDiv
              key={location.pathname}
              variants={pageVariants}
              initial="initial"
              animate="animate"
              exit="exit"
              transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
              className={styles.page}
            >
              <Outlet />
            </MotionDiv>
          </AnimatePresence>
        </div>
      </div>
    </div>
  )
}
