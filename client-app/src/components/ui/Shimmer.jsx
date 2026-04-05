import styles from './Shimmer.module.css'

export default function Shimmer({ height = 120, width = '100%', radius = 'var(--radius-sm)', className = '' }) {
  return (
    <div
      className={`shimmer ${styles.block} ${className}`}
      style={{ height, width, borderRadius: radius }}
    />
  )
}

export function ShimmerPage() {
  return (
    <div className={styles.page}>
      <Shimmer height={28} width="35%" />
      <div className={styles.row}>
        <Shimmer height={120} />
        <Shimmer height={120} />
        <Shimmer height={120} />
      </div>
      <Shimmer height={200} />
      <div className={styles.row}>
        <Shimmer height={16} width="70%" />
        <Shimmer height={16} width="45%" />
      </div>
    </div>
  )
}
