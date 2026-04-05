import { motion } from 'framer-motion'
import styles from './KpiCard.module.css'

const statusColors = {
  good: 'var(--success)',
  ok: 'var(--warning)',
  bad: 'var(--danger)',
  neutral: 'var(--text-muted)',
}

export default function KpiCard({ label, value, target, unit = '', status = 'neutral', trend, delay = 0 }) {
  const color = statusColors[status] || statusColors.neutral

  return (
    <motion.div
      className={styles.card}
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay, ease: [0.4, 0, 0.2, 1] }}
    >
      <div className={styles.label}>{label}</div>
      <div className={styles.value} style={{ color }}>
        {unit === '$' && <span className={styles.unit}>$</span>}
        {value ?? '-'}
        {unit && unit !== '$' && <span className={styles.unit}>{unit}</span>}
      </div>
      {target !== undefined && target !== null && (
        <div className={styles.target}>
          Target: {unit === '$' ? '$' : ''}{target}{unit && unit !== '$' ? unit : ''}
        </div>
      )}
      {trend && (
        <div className={`${styles.trend} ${trend > 0 ? styles.up : styles.down}`}>
          {trend > 0 ? '+' : ''}{trend}%
        </div>
      )}
    </motion.div>
  )
}
