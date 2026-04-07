import { useEffect } from 'react'
import { motion } from 'framer-motion'
import styles from './Placeholder.module.css'

const MotionDiv = motion.div

export default function LegacyRedirectPage({ title, to, description }) {
  useEffect(() => {
    if (!to) return
    window.location.replace(to)
  }, [to])

  return (
    <MotionDiv
      className={styles.page}
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className={styles.inner}>
        <h1 className={styles.title}>{title}</h1>
        <p className={styles.desc}>
          {description || 'Opening the full editor...'}
        </p>
        <p className={styles.desc}>
          <a href={to}>Continue to {title}</a>
        </p>
      </div>
    </MotionDiv>
  )
}
