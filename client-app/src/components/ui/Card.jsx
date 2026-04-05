import styles from './Card.module.css'

export default function Card({ children, className = '', padding = true, ...props }) {
  return (
    <div
      className={`${styles.card} ${padding ? styles.padded : ''} ${className}`}
      {...props}
    >
      {children}
    </div>
  )
}

export function CardHeader({ title, subtitle, action, children }) {
  return (
    <div className={styles.header}>
      <div>
        <h3 className={styles.title}>{title}</h3>
        {subtitle && <p className={styles.subtitle}>{subtitle}</p>}
      </div>
      {action && <div className={styles.action}>{action}</div>}
      {children}
    </div>
  )
}

export function CardGrid({ children, cols = 3, className = '' }) {
  return (
    <div
      className={`${styles.grid} ${className}`}
      style={{ '--cols': cols }}
    >
      {children}
    </div>
  )
}
