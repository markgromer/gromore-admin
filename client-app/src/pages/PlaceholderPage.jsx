import { motion } from 'framer-motion'
import styles from './Placeholder.module.css'

export default function PlaceholderPage({ title, icon: Icon, description }) {
  return (
    <motion.div
      className={styles.page}
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className={styles.inner}>
        {Icon && <Icon size={48} strokeWidth={1.2} className={styles.icon} />}
        <h1 className={styles.title}>{title}</h1>
        <p className={styles.desc}>{description || 'This page is being built. Check back soon.'}</p>
      </div>
    </motion.div>
  )
}
